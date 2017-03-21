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
import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;

/**
 * Container for the bytes that represent an HBase Event Store column qualifier byte[] value
 */
public class ColumnQualifier implements Comparable<ColumnQualifier> {

    public static final int ARRAY_LENGTH = Integer.BYTES;

    private final byte[] bytes;
    private final int offset;

    //cached for performance, relies on the array not being mutated
    private final int hashCode;

    public ColumnQualifier(final byte[] bytes, final int offset) {
        //the backing array should have enough bytes from the offset to accommodate the col qual
        Preconditions.checkArgument(bytes.length - offset >= ARRAY_LENGTH);
        this.bytes = bytes;
        this.offset = offset;
        this.hashCode = buildHashCode();
    }

    private ColumnQualifier(final byte[] bytes) {
        this(bytes, 0);
    }

    private ColumnQualifier(final int value) {
        this(Bytes.toBytes(value));
    }

    public static ColumnQualifier from(final byte[] bytes) {
        return new ColumnQualifier(bytes);
    }

    public static ColumnQualifier from(final byte[] bytes, final int offset) {
        return new ColumnQualifier(bytes, offset);
    }

    public static ColumnQualifier from(final int value) {
        return new ColumnQualifier(value);
    }

    public byte[] getBackingArray() {
        return bytes;
    }

    public byte[] getBytes() {
        if (bytes.length == ARRAY_LENGTH) {
            return bytes;
        } else {
            return Bytes.copy(bytes, offset, ARRAY_LENGTH);
        }
    }

    /**
     * @return The offset that the ColumnQualifier starts at in the backing array
     */
    public int getOffset() {
        return offset;
    }

    public int getValue() {
        return Bytes.toInt(bytes, offset);
    }

    /**
     * @return The length of the ColumnQualifier itself, rather than the backing array which may be longer
     */
    public static int length() {
        return ARRAY_LENGTH;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int buildHashCode() {
        return Bytes.hashCode(bytes, offset, ARRAY_LENGTH);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final ColumnQualifier that = (ColumnQualifier) obj;
        //equality based only on the part of the backing array of interest
        if (!Bytes.equals(this.bytes, this.offset, ARRAY_LENGTH, that.bytes, that.offset, ARRAY_LENGTH)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return ByteArrayUtils.byteArrayToHex(bytes, offset, ARRAY_LENGTH);
    }

    /**
     * @return The array represented in hex, decimal and 'hbase' forms. The
     *         hbase form is mix of ascii and deciaml, so an ascii char if the
     *         byte value exists in the ascii table
     */
    public String toAllForms() {
        return ByteArrayUtils.byteArrayToAllForms(bytes, offset, ARRAY_LENGTH);
    }

    @Override
    public int compareTo(final ColumnQualifier that) {
        return Bytes.compareTo(this.bytes, this.offset, ARRAY_LENGTH, that.bytes, that.offset, ARRAY_LENGTH);
    }

    /**
     * Compare this ColumnQualifier to a portion of another array startign at a given offset
     * @param otherBytes
     * @param otherOffset
     * @return
     */
    public int compareTo(final byte[] otherBytes, final int otherOffset) {
        return Bytes.compareTo(this.bytes, this.offset, ARRAY_LENGTH, otherBytes, otherOffset, ARRAY_LENGTH);
    }

}
