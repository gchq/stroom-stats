

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

import com.google.common.base.Preconditions;

public class CellQualifier {
    private final RowKey rowKey;
    private final ColumnQualifier columnQualifier;
    private final long fullTimestamp;

    // cache of the hashcode as this object is immutable
    private final int hashCode;

    public CellQualifier(final RowKey rowKey, final ColumnQualifier columnQualifier, final long fullTimestamp) {
        Preconditions.checkNotNull(rowKey);
        Preconditions.checkNotNull(columnQualifier);
        Preconditions.checkArgument(fullTimestamp >=0);

        // RowKey is immutable so just take the reference rather than deep copy
        this.rowKey = rowKey;
        this.columnQualifier = columnQualifier;
        this.fullTimestamp = fullTimestamp;
        hashCode = buildHashCode();
    }

    public CellQualifier(final byte[] rowKey, byte[] columnQualifier, final long fullTimestamp) {
        this(new RowKey(rowKey), ColumnQualifier.from(columnQualifier), fullTimestamp);
    }

    public RowKey getRowKey() {
        return rowKey;
    }

    public ColumnQualifier getColumnQualifier() {
        return columnQualifier;
    }

    /**
     * @return This will be the original time of the event rounded to the time
     *         interval that was used to create the cell qualifier
     */
    public long getFullTimestamp() {
        return fullTimestamp;
    }

    @Override
    public String toString() {
        String retVal;

        retVal = "rowKey: " + rowKey.toString() + " col qualifier: [" + columnQualifier
                + "] originalTimestamp: " + fullTimestamp;

        return retVal;
    }

    private int buildHashCode() {
        int result = rowKey.hashCode();
        result = 31 * result + columnQualifier.hashCode();
        result = 31 * result + (int) (fullTimestamp ^ (fullTimestamp >>> 32));
        return result;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final CellQualifier that = (CellQualifier) o;

        if (fullTimestamp != that.fullTimestamp) return false;
        if (!rowKey.equals(that.rowKey)) return false;
        return columnQualifier.equals(that.columnQualifier);
    }
}
