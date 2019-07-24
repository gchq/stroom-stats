

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

import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The row key structure looks like this:
 *
 * <stat name UID><roll up bit mask><partial timestamp><tag1nameUID><tag1valUID>...<tagNnameUID><tagNvalUID>
 *
 * and the widths in bytes look like this
 *
 * <4><2><4><4><4>...<4><4>
 */
public class RowKey {
    private final TimeAgnosticRowKey timeAgnosticRowKey;
    private final byte[] partialTimestamp;

    // cache of the hashcode as this object is immutable
    private volatile int hashCodeValue = 0;

    public static final int PARTIAL_TIMESTAMP_ARRAY_LENGTH = 4;

    // helper variables that add various permutations of the above statics
    public static final int UID_ARRAY_LENGTH = UID.UID_ARRAY_LENGTH;
    public static final int UID_AND_BIT_MASK_LENGTH = UID.UID_ARRAY_LENGTH
            + TimeAgnosticRowKey.ROLL_UP_BIT_MASK_LENGTH;
    public static final int UID_AND_BIT_MASK_AND_TIME_LENGTH = UID_AND_BIT_MASK_LENGTH + PARTIAL_TIMESTAMP_ARRAY_LENGTH;

    public RowKey(final UID typeId, final byte[] rollUpBitMask, final byte[] partialTimestamp,
                  final List<RowKeyTagValue> sortedTagValuePairs) {
        this.timeAgnosticRowKey = new TimeAgnosticRowKey(typeId, rollUpBitMask, sortedTagValuePairs);
        this.partialTimestamp = partialTimestamp;
    }

    public RowKey(final TimeAgnosticRowKey timeAgnosticRowKey, final byte[] partialTimestamp) {
        this.timeAgnosticRowKey = timeAgnosticRowKey;
        this.partialTimestamp = partialTimestamp;

        // cache the hascode to save it being calculated each time
        hashCodeValue = buildHashCode();
    }

    /**
     * Constructor to build a RowKey object from the row key byte array as
     * pulled from HBase
     *
     * @param rowKey
     *            row key byte array as taken from a HBase row key in the
     *            EventStore table
     */
    public RowKey(final byte[] rowKey) {
        final UID typeId =UID.from(rowKey, 0);

        int startPosition = UID_ARRAY_LENGTH;
        int positionOfNextSection = UID_ARRAY_LENGTH + TimeAgnosticRowKey.ROLL_UP_BIT_MASK_LENGTH;

        final byte[] rollUpBitMask = Arrays.copyOfRange(rowKey, startPosition, positionOfNextSection);

        startPosition += TimeAgnosticRowKey.ROLL_UP_BIT_MASK_LENGTH;
        positionOfNextSection += PARTIAL_TIMESTAMP_ARRAY_LENGTH;

        this.partialTimestamp = Arrays.copyOfRange(rowKey, startPosition, positionOfNextSection);

        final List<RowKeyTagValue> sortedTagValuePairs = Collections
                .unmodifiableList(RowKeyTagValue.extractTagValuePairs(rowKey, positionOfNextSection, rowKey.length));

        this.timeAgnosticRowKey = new TimeAgnosticRowKey(typeId, rollUpBitMask, sortedTagValuePairs);

        hashCodeValue = buildHashCode();
    }

    public byte[] asByteArray() {
        int tagValueTotalLength = 0;

        for (final RowKeyTagValue pair : timeAgnosticRowKey.getTagValuePairs()) {
            tagValueTotalLength += pair.asByteArray().length;
        }

        final byte[] rowKey = new byte[timeAgnosticRowKey.getTypeId().length()
                + timeAgnosticRowKey.getRollUpBitMask().length + partialTimestamp.length + tagValueTotalLength];

        final ByteBuffer buffer = ByteBuffer.wrap(rowKey);
        buffer.put(timeAgnosticRowKey.getTypeId().getUidBytes());
        buffer.put(timeAgnosticRowKey.getRollUpBitMask());
        buffer.put(partialTimestamp);
        for (final RowKeyTagValue pair : timeAgnosticRowKey.getTagValuePairs()) {
            buffer.put(pair.asByteArray());
        }

        return buffer.array();
    }

    public UID getTypeId() {
        return timeAgnosticRowKey.getTypeId();
    }

    public byte[] getRollUpBitMask() {
        return timeAgnosticRowKey.getRollUpBitMask();
    }

    public byte[] getPartialTimestamp() {
        return partialTimestamp;
    }

    public List<RowKeyTagValue> getTagValuePairs() {
        return timeAgnosticRowKey.getTagValuePairs();
    }

    public TimeAgnosticRowKey getTimeAgnosticRowKey() {
        return timeAgnosticRowKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(ByteArrayUtils.byteArrayToHex(partialTimestamp));
        sb.append("] [");
        sb.append("timeAgnosticRowkey: ");
        sb.append(timeAgnosticRowKey.toString());
        sb.append("]");

        return sb.toString();
    }

    private int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(partialTimestamp);
        result = prime * result + ((timeAgnosticRowKey == null) ? 0 : timeAgnosticRowKey.hashCode());
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
        final RowKey other = (RowKey) obj;
        if (!Arrays.equals(partialTimestamp, other.partialTimestamp))
            return false;
        if (timeAgnosticRowKey == null) {
            if (other.timeAgnosticRowKey != null)
                return false;
        } else if (!timeAgnosticRowKey.equals(other.timeAgnosticRowKey))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        if (hashCodeValue == 0) {
            hashCodeValue = buildHashCode();
        }
        return hashCodeValue;
    }

    public static int calculateRowKeyLength(final int tagCount) {
        return UID_AND_BIT_MASK_AND_TIME_LENGTH + (tagCount * RowKeyTagValue.TAG_AND_VALUE_ARRAY_LENGTH);
    }
}
