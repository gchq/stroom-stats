/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

/*
 * This class is a modified form of OpenTSDB's (https://github.com/OpenTSDB/opentsdb)
 * UniqueId.java class.
 */

// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.

package stroom.stats.hbase.uid;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.exception.HBaseException;
import stroom.stats.hbase.table.UniqueIdForwardMapTable;
import stroom.stats.hbase.table.UniqueIdReverseMapTable;
import stroom.stats.hbase.util.bytes.UnsignedBytes;

import java.util.Optional;

public class UniqueId {
    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueId.class);

    /**
     * How many times do we try to assign an ID before giving up.
     */
    private static final short MAX_ATTEMPTS_ASSIGN_ID = 100;

    private final UniqueIdForwardMapTable forwardMapTable;
    private final UniqueIdReverseMapTable reverseMapTable;
    private final int width;
    private final long maxId;


    public UniqueId(final UniqueIdForwardMapTable forwardMapTable, final UniqueIdReverseMapTable reverseMapTable,
                    final int width) {
        this.forwardMapTable = forwardMapTable;
        this.reverseMapTable = reverseMapTable;
        this.width = width;
        this.maxId = UnsignedBytes.maxValue(width);

        LOGGER.debug("Max id for is {}", maxId);
    }

    public Optional<byte[]> getId(final String name) {
        final byte[] nameKey = Bytes.toBytes(name);
        return forwardMapTable.getId(nameKey);
    }

    public byte[] getOrCreateId(final String name) {
        short attempt = MAX_ATTEMPTS_ASSIGN_ID;

        Optional<byte[]> bId = Optional.empty();
        final byte[] bName = Bytes.toBytes(name);

        while (attempt-- > 0 && !bId.isPresent()) {
            try {
                // Try and get the Id/UID from the database
                bId = forwardMapTable.getId(bName);
            } catch (final Throwable t) {
                throw new RuntimeException(String.format("Error querying HBase for name {}", name, t));
            }

            if (!bId.isPresent()) {
                // Id didn't exist in the database so have a go at creating it,
                // in a thread safe way.

                // Grab the next Id from the sequence cell held at the top of
                // the table Ids are always increasing but may not be contiguous (e.g. if
                // a crash happens or if multiple threads
                // try to create a new ID at the same time where only one will
                // win). One option here would be to use NumberSequence to hold a
                // cache of say 10 IDs so reduce the number of
                // calls to the DB, however it is debatable how useful this would be
                // as once the system is in steady state the
                // number of new IDs generated will be low
                final long newId = reverseMapTable.nextId();
                LOGGER.trace("Got new ID={}", newId);

                if (newId > maxId) {
                    throw new IllegalStateException(
                            "Max id of " + maxId + " has been reached using " + width + " bytes");
                }

                // The new Id comes back as a long which is 8 bytes wide so we
                // convert it down to a smaller width as we don't need negative numbers and mostly to
                // save storage space
                final byte[] bNewId = convertToUid(newId, width);

                // If we die before the next Put succeeds, we just waste an ID.

                // Create the reverse mapping first, so that if we die before
                // creating the forward mapping we don't run the risk of
                // "publishing" a partially assigned ID. The reverse mapping on
                // its own is harmless but the forward mapping without reverse
                // mapping is bad.
                try {
                    // We are CAS'ing (Check And Set) the KV into existence --
                    // the second argument is how we tell HBase we want to
                    // atomically create the KV, so that if there is already a
                    // KV in this cell, we'll fail. Technically we could do just
                    // a `put' here, as we have a freshly allocated UID, so
                    // there is not reason why a KV should already exist for
                    // this UID, but just to err on the safe side and catch
                    // really weird corruption cases, we do a CAS instead to
                    // create the KV.
                    LOGGER.trace("Adding reverse mapping for ID={} for {}", newId, name);

                    if (!reverseMapTable.checkAndPutName(bNewId, bName)) {
                        // a name already exists for this UID, which should
                        // never happen as each thread would get
                        // a unique UID from the the call to nextId
                        LOGGER.error("Failed to CAS reverse mapping: {}", name);

                        throw new RuntimeException(
                                "Failed to CAS reverse mapping: " + name + "");
                    }

                } catch (final HBaseException e) {
                    LOGGER.error("Failed to CAS reverse mapping!  ID leaked: {} of kind {}", newId, e);
                    throw new RuntimeException(e.getMessage(), e);
                } catch (final Exception e) {
                    LOGGER.error("Should never be here!  ID leaked: {} of kind {}", newId, e);
                    throw new RuntimeException(e.getMessage(), e);
                }
                // If die before the next PutRequest succeeds, we just have an
                // "orphaned" reversed mapping, in other words a UID has been
                // allocated but never used and is not reachable, so it's just a
                // wasted UID. It does mean that there can be multiple rows in
                // the reverseMap table with the same name value but different
                // UIDs.

                // TODO Some form of back ground process to periodically check
                // that each entry in the reverseMap table has a corresponding
                // entry in the forwardMap table and if it doesn't delete it as
                // it is an orphan.

                // Now create the forward mapping.

                // Put the new id into the table.
                LOGGER.trace("Adding ID={} for {}", newId, name);

                if (!forwardMapTable.checkAndPutId(bName, bNewId)) {
                    // row with that name already exists in the forwardMap table
                    // meaning another thread beat us to it.
                    // Our UID is now redundant and the entry for it in the
                    // reverseMap table is orphaned so
                    // we need to remove it.
                    // Then we just go back round the loop again to pick up the
                    // UID that the other thread
                    // succeeded in fully creating.
                    LOGGER.warn(
                            "Race condition: tried to assign ID {} to {}, but CAS failed on {}, " +
                                    "which indicates this UID must have been allocated concurrently by another. " +
                                    "So ID {} was wasted and will be removed from the reverse map table.",
                            newId, name, newId, newId);

                    reverseMapTable.checkAndDeleteName(bNewId, bName);
                } else {
                    // both sides of the map were created so return the new UID
                    bId = Optional.of(bNewId);
                }

            } else {
                if (LOGGER.isDebugEnabled()) {
                    final long existingId = UnsignedBytes.get(bId.get(), 0, width);
                    LOGGER.trace("Found existing ID={} for {}", existingId, name);
                }
            }

            if (attempt == 0) {
                LOGGER.error("Tried {} times to getOrCreate a UID for name {}, giving up", MAX_ATTEMPTS_ASSIGN_ID, name);
                throw new RuntimeException(String.format("Tried %s times to getOrCreate a UID for name %s, giving up",
                        MAX_ATTEMPTS_ASSIGN_ID, name));
            }
        }
        // by this point we should either have a bId or an exception would have
        // been thrown
        return bId.get();
    }

    public Optional<String> getName(final byte[] id) {
        final Optional<byte[]> bName = reverseMapTable.getName(id);
        return bName.flatMap(bytes -> Optional.of(Bytes.toString(bytes)));
    }

    public int getWidth() {
        return width;
    }

    public static byte[] convertToUid(final long id, final int width) {
        final byte[] uid = new byte[width];

        UnsignedBytes.put(uid, 0, width, id);

        return uid;
    }

    private static byte[] getUidByOffset(final byte[] uid, final int width, final int offset) {
        long val = UnsignedBytes.get(uid, 0, width);
        val += offset;
        return UnsignedBytes.toBytes(width, val);
    }

    public static byte[] getNextUid(final byte[] uid, final int width) {
        return getUidByOffset(uid, width, +1);
    }

    public static byte[] getPrevUid(final byte[] uid, final int width) {
        return getUidByOffset(uid, width, -1);
    }
}
