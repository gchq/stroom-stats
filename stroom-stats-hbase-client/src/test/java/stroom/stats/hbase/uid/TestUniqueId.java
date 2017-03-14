

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

package stroom.stats.hbase.uid;

import org.junit.Assert;
import org.junit.Test;

import stroom.stats.hbase.util.bytes.UnsignedBytes;

public class TestUniqueId {
    private static final int BYTE_ARRAY_WIDTH = 4;

    @Test
    public void testConvertToUid() throws Exception {
        final long val = 867L;

        final byte[] bytes867 = UniqueId.convertToUid(val, BYTE_ARRAY_WIDTH);

        final byte[] expected = UnsignedBytes.toBytes(BYTE_ARRAY_WIDTH, val);

        Assert.assertArrayEquals(expected, bytes867);
    }

    @Test
    public void testGetNextUid() throws Exception {
        final long val = 867L;

        final byte[] bytes867 = UniqueId.convertToUid(val, BYTE_ARRAY_WIDTH);

        final byte[] bytes868 = UniqueId.getNextUid(bytes867, BYTE_ARRAY_WIDTH);

        final long nextVal = UnsignedBytes.get(bytes868, 0, BYTE_ARRAY_WIDTH);

        Assert.assertEquals(val + 1, nextVal);
    }

    @Test
    public void testGetPrevUid() throws Exception {
        final long val = 867L;

        final byte[] bytes867 = UniqueId.convertToUid(val, BYTE_ARRAY_WIDTH);

        final byte[] bytes866 = UniqueId.getPrevUid(bytes867, BYTE_ARRAY_WIDTH);

        final long nextVal = UnsignedBytes.get(bytes866, 0, BYTE_ARRAY_WIDTH);

        Assert.assertEquals(val - 1, nextVal);
    }
}
