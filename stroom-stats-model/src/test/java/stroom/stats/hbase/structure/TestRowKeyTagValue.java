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

package stroom.stats.hbase.structure;

import com.google.common.primitives.Bytes;
import org.junit.Test;
import stroom.stats.hbase.uid.UID;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRowKeyTagValue {

    UID tag1 = UID.from(new byte[]{0, 0, 0, 1});
    UID value1 = UID.from(new byte[]{0, 0, 1, 1});

    UID tag2 = UID.from(new byte[]{0, 0, 0, 2});
    UID value2 = UID.from(new byte[]{0, 0, 1, 2});

    @Test
    public void asByteArray() throws Exception {
        RowKeyTagValue tagValue = new RowKeyTagValue(tag1, value1);
        assertThat(tagValue.asByteArray()).isEqualTo(Bytes.concat(tag1.getUidBytes(), value1.getUidBytes()));
    }

    @Test
    public void extractTagValuePairs() throws Exception {

        byte[] paddedBytes = Bytes.concat(new byte[] {9,9},
                tag1.getUidBytes(),
                value1.getUidBytes(),
                tag2.getUidBytes(),
                value2.getUidBytes());
        int offset = 2;

        RowKeyTagValue tagValue1 = new RowKeyTagValue(tag1, value1);
        RowKeyTagValue tagValue2 = new RowKeyTagValue(tag2, value2);
        List<RowKeyTagValue> tagValues = RowKeyTagValue.extractTagValuePairs(paddedBytes, offset, offset + UID.UID_ARRAY_LENGTH * 4);
        assertThat(tagValues).hasSize(2);
        assertThat(tagValues.get(0)).isEqualTo(tagValue1);
        assertThat(tagValues.get(1)).isEqualTo(tagValue2);
        assertThat(tagValues.get(0).hashCode()).isEqualTo(tagValue1.hashCode());
        assertThat(tagValues.get(1).hashCode()).isEqualTo(tagValue2.hashCode());
    }

    @Test
    public void isTagUidKnown() throws Exception {
        RowKeyTagValue tvKnownTag = new RowKeyTagValue(tag1, value1);
        RowKeyTagValue tvUnknownTag = new RowKeyTagValue(UID.NOT_FOUND_UID, value1);

        assertThat(tvKnownTag.isTagUidKnown()).isTrue();
        assertThat(tvUnknownTag.isTagUidKnown()).isFalse();
    }

    @Test
    public void isValueUidKnown() throws Exception {
        RowKeyTagValue tvKnownValue = new RowKeyTagValue(tag1, value1);
        RowKeyTagValue tvUnknownValue = new RowKeyTagValue(tag1, UID.NOT_FOUND_UID);

        assertThat(tvKnownValue.isValueUidKnown()).isTrue();
        assertThat(tvUnknownValue.isValueUidKnown()).isFalse();

    }

    @Test
    public void areTagAndValueUidsKnown() throws Exception {
        RowKeyTagValue tvBothKnown = new RowKeyTagValue(tag1, value1);
        RowKeyTagValue tvUnknownValue = new RowKeyTagValue(tag1, UID.NOT_FOUND_UID);
        RowKeyTagValue tvUnknownTag = new RowKeyTagValue(UID.NOT_FOUND_UID, value1);
        RowKeyTagValue tvBothUnknown = new RowKeyTagValue(UID.NOT_FOUND_UID, UID.NOT_FOUND_UID);

        assertThat(tvBothKnown.areTagAndValueUidsKnown()).isTrue();
        assertThat(tvUnknownTag.areTagAndValueUidsKnown()).isFalse();
        assertThat(tvUnknownValue.areTagAndValueUidsKnown()).isFalse();
        assertThat(tvBothUnknown.areTagAndValueUidsKnown()).isFalse();
    }

    @Test
    public void testHashCode() throws Exception {
        RowKeyTagValue tagValue1 = new RowKeyTagValue(tag1, value1);
        RowKeyTagValue tagValue2 = new RowKeyTagValue(tag2, value2);

        assertThat(tagValue1.hashCode()).isNotEqualTo(tagValue2);

    }

    @Test
    public void equals() throws Exception {
        //tested above
    }

    @Test
    public void shallowCopy() throws Exception {
        RowKeyTagValue tagValue1 = new RowKeyTagValue(tag1, value1);
        RowKeyTagValue tagValue2 = tagValue1.shallowCopy();

        assertThat(tagValue1).isEqualTo(tagValue2);
        assertThat(System.identityHashCode(tagValue1)).isNotEqualTo(System.identityHashCode(tagValue2));
        assertThat(System.identityHashCode(tagValue1.getTag())).isEqualTo(System.identityHashCode(tagValue2.getTag()));
        assertThat(System.identityHashCode(tagValue1.getValue())).isEqualTo(System.identityHashCode(tagValue2.getValue()));
    }

    @Test
    public void compareTo() throws Exception {

        assertThat(doCompare(tag1, value1, tag1, value1)).isEqualTo(0);

        assertThat(doCompare(tag1, value1, tag2, value1)).isLessThan(0);
        assertThat(doCompare(tag2, value1, tag1, value1)).isGreaterThan(0);

        assertThat(doCompare(tag1, value1, tag1, value2)).isLessThan(0);
        assertThat(doCompare(tag1, value2, tag1, value1)).isGreaterThan(0);
    }

    private int doCompare(UID t1, UID v1, UID t2, UID v2) {
        return new RowKeyTagValue(t1, v1).compareTo(new RowKeyTagValue(t2, v2));
    }

}