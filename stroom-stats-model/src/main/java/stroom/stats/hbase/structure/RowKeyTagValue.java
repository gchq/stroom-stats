

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
import stroom.stats.hbase.uid.UID;

import java.util.ArrayList;
import java.util.List;

public class RowKeyTagValue implements TagValueFilterTreeNode, Comparable<RowKeyTagValue> {
    public static final int TAG_AND_VALUE_ARRAY_LENGTH = UID.UID_ARRAY_LENGTH * 2;

    private final UID tag;

    private final UID value;

    private volatile byte[] concatenatedByteArray;

    private final Object privateLock = new Object();

    private volatile int hashCode = 0;

    public RowKeyTagValue(final UID tag, final UID value) {
        Preconditions.checkNotNull(tag);
        Preconditions.checkNotNull(value);
        this.tag = tag;
        this.value = value;
    }

//    /**
//     * Constructor. It is allowed for the value to be null as this tag may not
//     * have a value or the filter term may want a tag with no value. The byte
//     * value will be null if the corresponding string value is not found in the
//     * UID table/cache
//     *
//     * @param tag
//     *            UID byte[] representing the tag string
//     * @param value
//     *            UID byte[] representing the value string
//     */
//    public RowKeyTagValue(final byte[] tag, final byte[] value) {
//        if (tag == null) {
//            throw new RuntimeException("Cannot instantiate a RowKeyTagValue object with a null tag. tag: ["
//                    + ByteArrayUtils.byteArrayToHex(tag) + "]");
//        }
//        if (tag.length != RowKey.UID_ARRAY_LENGTH || (value != null && value.length != RowKey.UID_ARRAY_LENGTH)) {
//            throw new RuntimeException("tag or value argument has an invalid array length, should be "
//                    + RowKey.UID_ARRAY_LENGTH + " each. tag: [" + ByteArrayUtils.byteArrayToHex(tag) + "] Value: ["
//                    + ByteArrayUtils.byteArrayToHex(value) + "]");
//        }
//        this.tag = tag;
//        this.value = value;
//
//    }

//    public RowKeyTagValue(final byte[] tagValuePair) {
//        if (tagValuePair == null) {
//            throw new RuntimeException("Cannot instantiate a RowKeyTagValue object with a null value. TagValue: ["
//                    + ByteArrayUtils.byteArrayToHex(tagValuePair) + "]");
//        }
//        if (tagValuePair.length != (RowKey.UID_ARRAY_LENGTH * 2) && tagValuePair.length != RowKey.UID_ARRAY_LENGTH) {
//            throw new RuntimeException("tagValuePair argument has an invalid array length, should either be "
//                    + RowKey.UID_ARRAY_LENGTH + " or " + (RowKey.UID_ARRAY_LENGTH * 2) + ". tagValuePair: ["
//                    + ByteArrayUtils.byteArrayToHex(tagValuePair) + "]");
//        }
//        this.tag = Arrays.copyOf(tagValuePair, RowKey.UID_ARRAY_LENGTH);
//        this.value = Arrays.copyOfRange(tagValuePair, RowKey.UID_ARRAY_LENGTH, tagValuePair.length);
//    }

    public byte[] asByteArray() {
        // keep a cached copy of the concatenated array to improve the
        // performance of the sorts
        if (concatenatedByteArray == null) {
            synchronized (privateLock) {
                if (concatenatedByteArray == null) {
                    concatenatedByteArray = concatenateTagAndValue(this.tag.getUidBytes(), this.value.getUidBytes());
                }
            }
        }
        return concatenatedByteArray;
    }

    /**
     * @param arr
     * @param startPos
     * @param endPos
     *            endPos is exclusive
     * @return
     */
    public static List<RowKeyTagValue> extractTagValuePairs(final byte[] arr, final int startPos, final int endPos) {
        // if the length of the pairs is not exactly divisible by the UID
        // length then the row key is bad
        if ((endPos - startPos) % UID.UID_ARRAY_LENGTH > 0) {
            throw new RuntimeException(
                    String.format("The supplied range (%s to %s) is an invalid number of bytes for the tag value pairs",
                            startPos, endPos));
        }

        final List<RowKeyTagValue> tempList = new ArrayList<>();

        int pos = startPos;
        while (pos < endPos) {
            // build a tagValue object from the byte array and add it to the
            // list
            UID tag = UID.from(arr, pos);
            pos += UID.UID_ARRAY_LENGTH;
            UID value = UID.from(arr, pos);
            pos += UID.UID_ARRAY_LENGTH;
            tempList.add(new RowKeyTagValue(tag, value));
        }
        return tempList;
    }

    public UID getTag() {
        return tag;
    }

    public UID getValue() {
        return value;
    }

    public boolean isTagUidKnown() {
        return !tag.equals(UID.NOT_FOUND_UID);
    }

    public boolean isValueUidKnown() {
        return !value.equals(UID.NOT_FOUND_UID);
    }

    public boolean areTagAndValueUidsKnown() {
        return (!tag.equals(UID.NOT_FOUND_UID)
                && !value.equals(UID.NOT_FOUND_UID));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(tag);
        sb.append("] [");
        sb.append(value);
        sb.append("]");

        return sb.toString();
    }

    private byte[] concatenateTagAndValue(final byte[] tag, final byte[] value) {
        final byte[] retVal = new byte[tag.length + (value == null ? RowKey.UID_ARRAY_LENGTH : value.length)];
        System.arraycopy(tag, 0, retVal, 0, tag.length);
        if (value != null) {
            System.arraycopy(value, 0, retVal, tag.length, value.length);
        }

        return retVal;
    }

    @Override
    public void printNode(final StringBuilder sb) {
        sb.append(toString());

    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = buildHashCode();
        }
        return hashCode;
    }

    public int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + tag.hashCode();
        result = prime * result + value.hashCode();
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof RowKeyTagValue))
            return false;
        final RowKeyTagValue other = (RowKeyTagValue) obj;
        if (!tag.equals(other.tag))
            return false;
        if (!value.equals(other.value))
            return false;
        return true;
    }

    public RowKeyTagValue shallowCopy() {
        return new RowKeyTagValue(tag, value);
    }

    @Override
    public int compareTo(final RowKeyTagValue that) {
        int tagResult = tag.compareTo(that.tag);
        if (tagResult == 0) {
            return value.compareTo(that.value);
        } else {
            return tagResult;
        }
    }
}
