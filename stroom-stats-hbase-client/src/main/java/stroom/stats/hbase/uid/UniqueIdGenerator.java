/*
 * Copyright 2017 Crown Copyright
 *
 * This file is part of Stroom-Stats.
 *
 * Stroom-Stats is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stroom-Stats is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stroom-Stats.  If not, see <http://www.gnu.org/licenses/>.
 */

package stroom.stats.hbase.uid;

import stroom.stats.hbase.util.bytes.UnsignedBytes;

import java.util.Optional;

public interface UniqueIdGenerator {

    static byte[] convertToUid(long id, int width) {
        final byte[] uid = new byte[width];

        UnsignedBytes.put(uid, 0, width, id);

        return uid;
    }

    static byte[] getNextUid(byte[] uid, int width) {
        return getUidByOffset(uid, width, +1);
    }

    static byte[] getPrevUid(byte[] uid, int width) {
        return getUidByOffset(uid, width, -1);
    }

    static byte[] getUidByOffset(final byte[] uid, final int width, final int offset) {
        long val = UnsignedBytes.get(uid, 0, width);
        val += offset;
        return UnsignedBytes.toBytes(width, val);
    }

    Optional<byte[]> getId(String name);

    byte[] getOrCreateId(String name);

    Optional<String> getName(byte[] id);

    int getWidth();
}
