

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class represents the parts of a {@link RowKey} that are not dependent on
 * the event time. It can be used as a generic skeleton for building a full
 * {@link RowKey}
 */
public class TimeAgnosticRowKey {
    private final UID typeId;
    private final byte[] rollUpBitMask;
    private final List<RowKeyTagValue> sortedTagValuePairs;

    private final int hashCode;

    public static final int UID_ARRAY_LENGTH = UID.UID_ARRAY_LENGTH;
    public static final int ROLL_UP_BIT_MASK_LENGTH = 2;

    public TimeAgnosticRowKey(final UID typeId, final byte[] rollUpBitMask,
            final List<RowKeyTagValue> sortedTagValuePairs) {
        this.typeId = typeId;
        this.rollUpBitMask = rollUpBitMask;

        // conver the list of tags to an unmodifiable list
        final List<RowKeyTagValue> tempList = new ArrayList<>();
        for (final RowKeyTagValue rowKeyTagValue : sortedTagValuePairs) {
            tempList.add(rowKeyTagValue.shallowCopy());
        }
        this.sortedTagValuePairs = Collections.unmodifiableList(tempList);
        hashCode = buildHashCode();
    }

    public UID getTypeId() {
        return this.typeId;
    }

    public List<RowKeyTagValue> getTagValuePairs() {
        return this.sortedTagValuePairs;
    }

    public byte[] getRollUpBitMask() {
        return rollUpBitMask;
    }

    /**
     * @return Just the TypeId and rollUpMask parts
     */
    public byte[] asPartialKey() {
        byte[] partialKey = new byte[UID_ARRAY_LENGTH + ROLL_UP_BIT_MASK_LENGTH];
        ByteBuffer byteBuffer = ByteBuffer.wrap(partialKey);
        byteBuffer.put(typeId.getBackingArray(), typeId.getOffset(), UID.length());
        byteBuffer.put(rollUpBitMask);

        return byteBuffer.array();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(typeId);
        sb.append("] [");
        sb.append(ByteArrayUtils.byteArrayToHex(rollUpBitMask));
        sb.append("] [");

        for (final RowKeyTagValue rowKeyTagValue : sortedTagValuePairs) {
            sb.append("[");
            sb.append(rowKeyTagValue.toString());
            sb.append("]");
        }
        sb.append("]");

        return sb.toString();
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(rollUpBitMask);
        result = prime * result + ((sortedTagValuePairs == null) ? 0 : sortedTagValuePairs.hashCode());
        result = prime * result + typeId.hashCode();
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
        final TimeAgnosticRowKey other = (TimeAgnosticRowKey) obj;
        if (!Arrays.equals(rollUpBitMask, other.rollUpBitMask))
            return false;
        if (sortedTagValuePairs == null) {
            if (other.sortedTagValuePairs != null)
                return false;
        } else if (!sortedTagValuePairs.equals(other.sortedTagValuePairs))
            return false;
        if (!typeId.equals(other.typeId))
            return false;
        return true;
    }
}
