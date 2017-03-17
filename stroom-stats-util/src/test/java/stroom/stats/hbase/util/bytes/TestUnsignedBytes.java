

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

package stroom.stats.hbase.util.bytes;

import org.junit.Assert;
import org.junit.Test;

public class TestUnsignedBytes {
    @Test
    public void testPutAndGet() {
        Assert.assertEquals(0, putAndGet(0));
        Assert.assertEquals(8, putAndGet(8));
        Assert.assertEquals(1000, putAndGet(1000));

        Assert.assertEquals(Integer.MAX_VALUE, putAndGet(Integer.MAX_VALUE));

        Assert.assertEquals(0, putAndGet(0, 4));
        Assert.assertEquals(8, putAndGet(8, 4));
        Assert.assertEquals(1000, putAndGet(1000, 4));

        Assert.assertEquals(Integer.MAX_VALUE, putAndGet(Integer.MAX_VALUE, 4));

        //
        // Assert.assertEquals(Integer.MAX_VALUE, putAndGet(Integer.MAX_VALUE,
        // 3));
        //
        // Assert.assertEquals(-1, putAndGet(-1));
        // Assert.assertEquals(Integer.MIN_VALUE, putAndGet(Integer.MIN_VALUE));
        //
        // Assert.assertEquals(-1, putAndGet(-1, 4));
        // Assert.assertEquals(Integer.MIN_VALUE, putAndGet(Integer.MIN_VALUE,
        // 4));
    }

    @Test
    public void testMaxValues() {
        for (int i = 1; i <= 8; i++) {
            final long max = UnsignedBytes.maxValue(i);
            System.out.println("Max value for " + i + " bytes = " + max);
            Assert.assertEquals(0, putAndGet(0, i));
            Assert.assertEquals(max - 1, putAndGet(max - 1, i));
            Assert.assertEquals(max, putAndGet(max, i));

            // Check for values that exceed bounds.
            IllegalArgumentException exception = null;
            try {
                putAndGet(max + 1, i);
            } catch (final IllegalArgumentException e) {
                exception = e;
            }
            Assert.assertNotNull(exception);
        }
    }

    @Test
    public void testWidth() {
        for (int i = 1; i <= 8; i++) {
            final long max = UnsignedBytes.maxValue(i);
            final int len = UnsignedBytes.requiredLength(max);

            Assert.assertEquals(i, len);
        }
    }

    private long putAndGet(final long in) {
        return putAndGet(in, 8);
    }

    private long putAndGet(final long in, final int len) {
        final byte[] bytes = new byte[len];
        UnsignedBytes.put(bytes, 0, len, in);
        return UnsignedBytes.get(bytes, 0, len);
    }
}
