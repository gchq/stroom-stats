

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

import java.util.Arrays;

import stroom.stats.hbase.util.bytes.ByteArrayUtils;

public class CellQualifier {
    private final RowKey rowKey;
    private final byte[] columnQualifier;
    private final long fullTimestamp;

    // cache of the hashcode as this object is immutable
    private final int hashCodeValue;

    public CellQualifier(final RowKey rowKey, final byte[] columnQualifier, final long fullTimestamp) {
        // RowKey is immutable so just take the reference rather than deep copy
        this.rowKey = rowKey;
        this.columnQualifier = columnQualifier;
        this.fullTimestamp = fullTimestamp;
        hashCodeValue = buildHashCode();
    }

    public CellQualifier(final byte[] rowKey, final byte[] columnQualifier, final long fullTimestamp) {
        this.rowKey = new RowKey(rowKey);
        this.columnQualifier = columnQualifier;
        this.fullTimestamp = fullTimestamp;
        hashCodeValue = buildHashCode();
    }

    public RowKey getRowKey() {
        return rowKey;
    }

    public byte[] getColumnQualifier() {
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

        retVal = "rowKey: " + rowKey.toString() + " col qualifier: [" + ByteArrayUtils.byteArrayToHex(columnQualifier)
                + "] originalTimestamp: " + fullTimestamp;

        return retVal;
    }

    @Override
    public int hashCode() {
        return hashCodeValue;
    }

    private int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(columnQualifier);
        result = prime * result + (int) (fullTimestamp ^ (fullTimestamp >>> 32));
        result = prime * result + ((rowKey == null) ? 0 : rowKey.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof CellQualifier))
            return false;
        final CellQualifier other = (CellQualifier) obj;
        if (!Arrays.equals(columnQualifier, other.columnQualifier))
            return false;
        if (fullTimestamp != other.fullTimestamp)
            return false;
        if (rowKey == null) {
            if (other.rowKey != null)
                return false;
        } else if (!rowKey.equals(other.rowKey))
            return false;
        return true;
    }
}
