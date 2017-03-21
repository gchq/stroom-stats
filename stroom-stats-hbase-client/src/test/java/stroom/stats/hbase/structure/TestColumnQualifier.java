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

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestColumnQualifier {

    byte[] bytes = new byte[]{1, 2, 3, 4};
    byte[] bigBytes = new byte[]{9, 9, 1, 2, 3, 4, 8, 8};
    byte[] otherBytes = new byte[] {4,3,2,1};
    int bigBytesOffset = 2;

    @Test
    public void of() throws Exception {
        ColumnQualifier columnQualifier = ColumnQualifier.from(bytes);
        Assertions.assertThat(columnQualifier.getBytes()).isEqualTo(bytes);
        Assertions.assertThat(columnQualifier.getBackingArray()).isEqualTo(bytes);
    }

    @Test
    public void of_withOffset() throws Exception {
        ColumnQualifier columnQualifier = ColumnQualifier.from(bigBytes, bigBytesOffset);
        Assertions.assertThat(columnQualifier.getBytes()).isEqualTo(bytes);
        Assertions.assertThat(columnQualifier.getBackingArray()).isEqualTo(bigBytes);
    }

    @Test
    public void getBackingArray() throws Exception {
        //tested above
    }

    @Test
    public void getBytes() throws Exception {
        //tested above
    }

    @Test
    public void testHashCode() throws Exception {
        ColumnQualifier columnQualifier1 = ColumnQualifier.from(bytes);
        ColumnQualifier columnQualifier2 = ColumnQualifier.from(bigBytes, bigBytesOffset);

        Assertions.assertThat(columnQualifier1).isEqualTo(columnQualifier2);
        Assertions.assertThat(columnQualifier1.hashCode()).isEqualTo(columnQualifier2.hashCode());
    }

    @Test
    public void equals() throws Exception {
        ColumnQualifier columnQualifier1 = ColumnQualifier.from(bytes);
        ColumnQualifier columnQualifier2 = ColumnQualifier.from(otherBytes);

        Assertions.assertThat(columnQualifier1).isNotEqualTo(columnQualifier2);
    }

    @Test
    public void compareTo() throws Exception {
        ColumnQualifier columnQualifier1 = ColumnQualifier.from(bytes);
        ColumnQualifier columnQualifier2 = ColumnQualifier.from(otherBytes);
        ColumnQualifier columnQualifier3 = ColumnQualifier.from(bigBytes, bigBytesOffset);

        Assertions.assertThat(columnQualifier1.compareTo(columnQualifier2)).isLessThan(0);
        Assertions.assertThat(columnQualifier1.compareTo(columnQualifier3)).isEqualTo(0);
    }

    @Test
    public void compareTo_otherBytes() throws Exception {
        ColumnQualifier columnQualifier1 = ColumnQualifier.from(bytes);

        Assertions.assertThat(columnQualifier1.compareTo(bigBytes, bigBytesOffset)).isEqualTo(0);
        Assertions.assertThat(columnQualifier1.compareTo(otherBytes, 0)).isLessThan(0);
    }
}