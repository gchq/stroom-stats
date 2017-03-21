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

import org.junit.Test;
import stroom.stats.hbase.uid.UID;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCellQualifier {

    UID statName1 = UID.from(new byte[] {0,0,0,1});
    byte[] time1 = new byte[] {8,7,6,5};
    byte[] rollupMask1 = new byte[] {0,0};

    RowKeyTagValue rowKeyTagValue1 = new RowKeyTagValue(UID.from(new byte[] {0,0,1,1}), UID.from(new byte[] {0,0,1,2}) );

    RowKey rowKey1 = new RowKey(statName1, rollupMask1, time1, Collections.singletonList(rowKeyTagValue1));
    byte[] bytes1 = new byte[] {9,9,2,2,2,2,9,9};
    ColumnQualifier columnQualifier1 = new ColumnQualifier(bytes1, 2);

    UID statName2 = UID.from(new byte[] {0,0,0,1});
    byte[] time2 = new byte[] {8,7,6,5};
    byte[] rollupMask2 = new byte[] {0,0};

    RowKeyTagValue rowKeyTagValue2 = new RowKeyTagValue(UID.from(new byte[] {0,0,1,1}), UID.from(new byte[] {0,0,1,2}) );

    RowKey rowKey2 = new RowKey(statName2, rollupMask2, time2, Collections.singletonList(rowKeyTagValue2));
    byte[] bytes2 = new byte[] {9,9,9,9,2,2,2,2};
    ColumnQualifier columnQualifier2 = new ColumnQualifier(bytes2, 4);

    @Test
    public void equals_equals() throws Exception {

        CellQualifier cellQualifier1 = new CellQualifier(rowKey1, columnQualifier1, 1234L);
        CellQualifier cellQualifier2 = new CellQualifier(rowKey2, columnQualifier2, 1234L);

        assertThat(cellQualifier1).isEqualTo(cellQualifier2);
        assertThat(cellQualifier1.hashCode()).isEqualTo(cellQualifier2.hashCode());
    }

    @Test
    public void equals_NotEquals() throws Exception {

        CellQualifier cellQualifier1 = new CellQualifier(rowKey1, columnQualifier1, 1234L);
        CellQualifier cellQualifier2 = new CellQualifier(rowKey2, columnQualifier2, 9999L);

        assertThat(cellQualifier1).isNotEqualTo(cellQualifier2);
        assertThat(cellQualifier1.hashCode()).isNotEqualTo(cellQualifier2.hashCode());
    }

}