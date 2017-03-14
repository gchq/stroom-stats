

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
 *
 *
 */

package stroom.stats.hbase.structure;

import stroom.stats.hbase.uid.UID;

import java.nio.ByteBuffer;
import java.util.List;

public class RowKeyUtils {
    private RowKeyUtils() {
        // Only static methods
    }

    public static byte[] tagValuePairsToBytes(final List<RowKeyTagValue> pairs) {
        if (pairs != null) {
            final byte[] output = new byte[pairs.size() * UID.UID_ARRAY_LENGTH * 2];

            final ByteBuffer byteBuffer = ByteBuffer.wrap(output);

            pairs.forEach(pair -> {
                byteBuffer.put(pair.getTag().getUidBytes());
                byteBuffer.put(pair.getValue().getUidBytes());
            });
            return output;
        } else {
            return new byte[0];
        }
    }

    /**
     * Concatenates the passed list of byte arrays into a single array
     *
     * @param parts
     *            A varargs list of byte array parts to concatenate together in
     *            the order in which they are passed
     * @return A single byte array made up of all the passed parts
     */
    public static byte[] constructByteArrayFromParts(final byte[]... parts) {
        if (parts == null || parts.length == 0) {
            throw new RuntimeException("Expecting at least one array part");
        }
        int arrSize = 0;

        for (final byte[] part : parts) {
            if (part != null) {
                arrSize += part.length;
            }
        }

        final byte[] output = new byte[arrSize];

        final ByteBuffer byteBuffer = ByteBuffer.wrap(output);

        for (final byte[] part : parts) {
            if (part != null) {
                byteBuffer.put(part);
            }
        }

        return output;
    }

    public static long roundTimeToBucketSize(final long timestamp, final long bucketSize) {
        final long bucketsSinceEpoch = timestamp / bucketSize;
        final long roundedTime = bucketsSinceEpoch * bucketSize;
        return roundedTime;
    }
}
