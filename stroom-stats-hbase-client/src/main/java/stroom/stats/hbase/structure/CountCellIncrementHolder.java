

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

import stroom.stats.hbase.util.bytes.ByteArrayUtils;

public class CountCellIncrementHolder {
    private final byte[] columnQualifier;
    private final long cellIncrementValue;

    /**
     * Creates an immutable holder object to hold a HBase column qualifier and
     * the long value to increment the cell by
     *
     * @param columnQualifier
     *            The column the increment operation applies to
     * @param cellValue
     *            The amount to increment the column by
     */
    public CountCellIncrementHolder(final byte[] columnQualifier, final long cellValue) {
        this.columnQualifier = columnQualifier;
        this.cellIncrementValue = cellValue;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public long getCellIncrementValue() {
        return cellIncrementValue;
    }

    @Override
    public String toString() {
        return "[" + ByteArrayUtils.byteArrayToHex(columnQualifier) + " - " + cellIncrementValue + "]";
    }
}
