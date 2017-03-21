

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

public class CountCellIncrementHolder {
    private final ColumnQualifier columnQualifier;
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
    public CountCellIncrementHolder(final ColumnQualifier columnQualifier, final long cellValue) {
        this.columnQualifier = columnQualifier;
        this.cellIncrementValue = cellValue;
    }

    public ColumnQualifier getColumnQualifier() {
        return columnQualifier;
    }

    public long getCellIncrementValue() {
        return cellIncrementValue;
    }

    public boolean areQualifiersEqual(CountCellIncrementHolder that) {
        return this.columnQualifier.equals(that);
    }

    @Override
    public String toString() {
        return "[" + columnQualifier + " - " + cellIncrementValue + "]";
    }
}
