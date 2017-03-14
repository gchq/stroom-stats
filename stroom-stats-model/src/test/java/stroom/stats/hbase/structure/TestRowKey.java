

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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.hbase.uid.UID;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRowKey {
    private static final UID STAT_TYPE_ID = UID.from(new byte[] { 0, 0, 0, 1 });
    private static final byte[] ROLL_UP_BIT_MASK = new byte[] { 0, 0 };
    private static final byte[] PARTIAL_TIMESTAMP = new byte[] { 9, 8, 7, 6 };
    private static final UID TAG_1 = UID.from(new byte[] { 1, 0, 0, 1 });
    private static final UID VALUE_1 = UID.from(new byte[] { 1, 0, 0, 2 });
    private static final UID TAG_2 = UID.from(new byte[] { 2, 0, 0, 1 });
    private static final UID VALUE_2 = UID.from(new byte[] { 2, 0, 0, 2 });
    private static final UID TAG_3 = UID.from(new byte[] { 3, 0, 0, 1 });
    private static final UID VALUE_3 = UID.from(new byte[] { 3, 0, 0, 2 });

    @Test
    public void testConstructorFromConstituentParts() {
        final List<RowKeyTagValue> tagValuePairs = new ArrayList<>();
        tagValuePairs.add(new RowKeyTagValue(TAG_2, VALUE_2));
        tagValuePairs.add(new RowKeyTagValue(TAG_1, VALUE_1));
        tagValuePairs.add(new RowKeyTagValue(TAG_3, VALUE_3));

        final RowKey rowKey = new RowKey(STAT_TYPE_ID, ROLL_UP_BIT_MASK, PARTIAL_TIMESTAMP, tagValuePairs);

        assertTrue(Arrays.equals(buildRowKeyArray(), rowKey.asByteArray()));
    }

    @Test
    public void testConstructorFromByteArray() {
        final RowKey rowKey = new RowKey(buildRowKeyArray());
        assertEquals(STAT_TYPE_ID, rowKey.getTypeId());
        Assertions.assertThat(ROLL_UP_BIT_MASK).isEqualTo(rowKey.getRollUpBitMask());
        Assertions.assertThat(PARTIAL_TIMESTAMP).isEqualTo(rowKey.getPartialTimestamp());

        final List<RowKeyTagValue> tagValuePairs = rowKey.getTagValuePairs();

        assertEquals(3, tagValuePairs.size());

        assertEquals(TAG_2, tagValuePairs.get(0).getTag());
        assertEquals(VALUE_2, tagValuePairs.get(0).getValue());
        assertEquals(TAG_1, tagValuePairs.get(1).getTag());
        assertEquals(VALUE_1, tagValuePairs.get(1).getValue());
        assertEquals(TAG_3, tagValuePairs.get(2).getTag());
        assertEquals(VALUE_3, tagValuePairs.get(2).getValue());
    }

    private int compareBytes(final byte[] a, final byte[] b) {
        int result = 0;
        final int maxLength = (a.length > b.length) ? a.length : b.length;
        for (int i = 0; i < maxLength; i++) {
            if (i == a.length - 1 && a.length < maxLength) {
                // same up to mismatch in length and b is longer so a < b
                result = -1;
                return result;
            }
            if (i == b.length - 1 && b.length < maxLength) {
                // same up to mismatch in length and a is longer so a > b
                result = 1;
                return result;
            }

            // compare value at position i
            if (a[i] != b[i]) {
                result = (a[i] > b[i]) ? 1 : -1;
                return result;
            }
        }
        return result;
    }

    private byte[] buildRowKeyArray() {
        final byte[] rowKey = new byte[RowKey.calculateRowKeyLength(3)];

        final ByteBuffer buffer = ByteBuffer.wrap(rowKey);
        buffer.put(STAT_TYPE_ID.getUidBytes());
        buffer.put(ROLL_UP_BIT_MASK);
        buffer.put(PARTIAL_TIMESTAMP);
        buffer.put(TAG_2.getUidBytes());
        buffer.put(VALUE_2.getUidBytes());
        buffer.put(TAG_1.getUidBytes());
        buffer.put(VALUE_1.getUidBytes());
        buffer.put(TAG_3.getUidBytes());
        buffer.put(VALUE_3.getUidBytes());

        return buffer.array();
    }
}
