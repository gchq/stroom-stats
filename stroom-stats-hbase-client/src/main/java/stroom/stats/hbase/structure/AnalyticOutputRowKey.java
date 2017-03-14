

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

import stroom.stats.hbase.uid.UID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Row Key object for the analytic output table. Row key takes the form
 *
 * <analytic name UID><timestamp><tag1 UID><tag1val UID>...<tagN UID><tagNval
 * UID><row number>
 *
 * The bytearray widths are as follows:
 *
 * <4><8><4><4>...<4><4><4>
 */
public class AnalyticOutputRowKey {
    private final UID analyticNameUid;
    private final byte[] runTimestamp;
    private final List<RowKeyTagValue> sortedTagValuePairs;
    private final byte[] rowNumber;

    public static final int UID_ARRAY_LENGTH = UID.UID_ARRAY_LENGTH;
    public static final int RUN_TIMESTAMP_ARRAY_LENGTH = 8;
    public static final int ROW_NUMBER_ARRAY_LENGTH = 4;

    public AnalyticOutputRowKey(final UID analyticNameUid, final byte[] runTimestamp,
            final List<RowKeyTagValue> sortedTagValuePairs, final byte[] rowNumber) {
        this.analyticNameUid = analyticNameUid;
        this.runTimestamp = runTimestamp;

        final List<RowKeyTagValue> tempList = new ArrayList<>();
        sortedTagValuePairs.forEach((pair) -> tempList.add(pair.shallowCopy()));
        this.sortedTagValuePairs = Collections.unmodifiableList(tempList);
        this.rowNumber = rowNumber;
    }

    public AnalyticOutputRowKey(final byte[] rowKey) {
        int pos = 0;
        this.analyticNameUid = UID.from(rowKey, pos);
        pos += UID_ARRAY_LENGTH;
        this.runTimestamp = Arrays.copyOfRange(rowKey, pos, pos += RUN_TIMESTAMP_ARRAY_LENGTH);

        final int arrLen = rowKey.length;
        this.sortedTagValuePairs = Collections
                .unmodifiableList(RowKeyTagValue.extractTagValuePairs(rowKey, pos, arrLen - ROW_NUMBER_ARRAY_LENGTH));
        this.rowNumber = Arrays.copyOfRange(rowKey, arrLen - ROW_NUMBER_ARRAY_LENGTH, arrLen);
    }

    public UID getAnalyticNameUid() {
        return analyticNameUid;
    }

    public byte[] getRunTimestamp() {
        return runTimestamp;
    }

    public byte[] getRowNumber() {
        return rowNumber;
    }

    public List<RowKeyTagValue> getSortedTagValuePairs() {
        return sortedTagValuePairs;
    }

    public byte[] asByteArray() {
        final byte[] tagValueSection = RowKeyUtils.tagValuePairsToBytes(sortedTagValuePairs);

        return RowKeyUtils.constructByteArrayFromParts(analyticNameUid.getUidBytes(), runTimestamp, tagValueSection, rowNumber);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + analyticNameUid.hashCode();
        result = prime * result + Arrays.hashCode(rowNumber);
        result = prime * result + Arrays.hashCode(runTimestamp);
        result = prime * result + ((sortedTagValuePairs == null) ? 0 : sortedTagValuePairs.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final AnalyticOutputRowKey other = (AnalyticOutputRowKey) obj;
        if (!analyticNameUid.equals(other.analyticNameUid))
            return false;
        if (!Arrays.equals(rowNumber, other.rowNumber))
            return false;
        if (!Arrays.equals(runTimestamp, other.runTimestamp))
            return false;
        if (sortedTagValuePairs == null) {
            if (other.sortedTagValuePairs != null)
                return false;
        } else if (!sortedTagValuePairs.equals(other.sortedTagValuePairs))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "AnalyticOutputRowKey [analyticNameUid=" + analyticNameUid + ", runTimestamp="
                + Arrays.toString(runTimestamp) + ", sortedTagValuePairs=" + sortedTagValuePairs + ", rowNumber="
                + Arrays.toString(rowNumber) + "]";
    }
}
