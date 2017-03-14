

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

package stroom.stats.hbase.util.bytes;

import java.util.Arrays;

/**
 * Wrapper class for a simple byte array so EHCache and other hashmaps can use
 * this wrapper as a key rather than the byte[]. This is because a byte[]
 * implements equality on an instance basis rather than a value basis
 *
 * Instances are immutable
 */
public class ByteArrayWrapper {
    private final byte[] bytes;
    private final int hashCode;

    private ByteArrayWrapper(final byte[] data) {
        // ensure immutable status
        this.bytes = Arrays.copyOf(data, data.length);
        // pre-compute the hashcode as this object is immutable
        this.hashCode = buildHashCode();
    }

    public static ByteArrayWrapper of(final byte[] data) {
        return new ByteArrayWrapper(data);
    }

    public byte[] getBytes() {
        // ensure immutable status
        return Arrays.copyOf(bytes, bytes.length);
        // return bytes;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final ByteArrayWrapper other = (ByteArrayWrapper) obj;
        if (!Arrays.equals(bytes, other.bytes))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return ByteArrayUtils.byteArrayToHex(bytes);
    }
}
