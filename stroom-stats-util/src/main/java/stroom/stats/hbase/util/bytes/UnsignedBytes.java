

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

public class UnsignedBytes {
    private static final byte[] ZERO_BYTES = new byte[0];

    public static byte[] toBytes(final int len, final long val) {
        if (len == 0 && val == 0) {
            return ZERO_BYTES;
        } else if (len < 1) {
            throw new IllegalArgumentException("You cannot use less than 1 byte to store a value.");
        }

        final byte[] bytes = new byte[len];
        put(bytes, 0, len, val);
        return bytes;
    }

    public static void put(final byte[] bytes, final int off, final int len, final long val) {
        if (val < 0) {
            throw new IllegalArgumentException("Negative values are not permitted.");
        }
        if (len > 8) {
            throw new IllegalArgumentException(
                    "You cannot use more than 8 bytes to store a value as only long values are supported.");
        }
        if (len < 1) {
            throw new IllegalArgumentException("You cannot use less than 1 byte to store a value.");
        }
        final long max = maxValue(len);
        if (val > max) {
            final StringBuilder sb = new StringBuilder();
            sb.append("Value ");
            sb.append(val);
            sb.append(" exceeds max value of ");
            sb.append(max);
            sb.append(" that can be stored in ");
            sb.append(len);
            sb.append(" byte");
            if (len > 1) {
                sb.append("s");
            }
            throw new IllegalArgumentException(sb.toString());
        }

        for (int i = 0; i < len; i++) {
            final int shift = (len - i - 1) * 8;
            bytes[off + i] = (byte) (val >> shift);
        }
    }

    public static long get(final byte[] bytes, final int off, final int len) {
        long val = 0;
        for (int i = 0; i < len; i++) {
            final int shift = (len - i - 1) * 8;
            val = val | ((long) bytes[off + i] & 0xff) << shift;
        }
        return val;
    }

    public static long maxValue(final int len) {
        if (len >= 8) {
            return Long.MAX_VALUE;
        }

        return (1L << (8 * len)) - 1;
    }

    public static int requiredLength(final long val) {
        if (val < 0) {
            throw new IllegalArgumentException("You must supply a positive value");
        } else if (val == 0) {
            return 0;
        }

        final long pos = 64 - Long.numberOfLeadingZeros(val) - 1;
        final double l = pos / 8D;
        final int len = (int) Math.ceil(l);

        return len;
    }
}
