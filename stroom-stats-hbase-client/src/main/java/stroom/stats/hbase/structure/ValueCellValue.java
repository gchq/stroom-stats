

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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

import stroom.stats.hbase.util.bytes.ByteArrayUtils;

/**
 * This immutable class holds the four values that make up a value cell in the
 * event store. Each value type cell is of the form
 *
 * <FF FF FF FF><FF FF FF FF FF FF FF FF><FF FF FF FF FF FF FF FF><FF FF FF FF
 * FF FF FF FF>
 *
 * i.e. <A><B><C><D>.
 *
 * Where:
 *
 * <A> is the count as an int
 *
 * <B> is the sum of all aggregated values as a double
 *
 * <C> is the minimum of all values
 *
 * <D> is the maximum of all values
 *
 * The constructors are used to convert back/forth between the byte and class.
 *
 * The reason for this structure is so that we can add multiple values into the
 * same time interval cell. Each time to add a new value we add it to the
 * existing value and increment the count. This means we can gradually aggregate
 * up the values, e.g. in capturing the number of bytes written to removable
 * media, we build up a total for that time interval. For something like cpu%
 * where a total is not appropriate we can use the count to determine the
 * average for all values added to the cell.
 *
 * Check and set type operations can be used to ensure we atomically change the
 * compound cell value and don't get two threads corrupting it
 */
public class ValueCellValue {
    private final int count;
    private final double aggregatedValue;
    private final double minValue;
    private final double maxValue;

    private static final int COUNT_BYTES_LENGTH = 4; // standard 4 byte width of
                                                        // an int
    private static final int VALUE_BYTES_LENGTH = 8; // standard 8 byte width of
                                                        // a double
    public static final int CELL_BYTE_ARRAY_LENGTH = COUNT_BYTES_LENGTH + (VALUE_BYTES_LENGTH * 3);

    public static ValueCellValue emptyInstance() {
        return new ValueCellValue(0, 0, 0, 0);
    }

    /**
     * Builds a {@link ValueCellValue} from the count and value supplied
     *
     * @param count
     *            The number of values added to the cell including this one
     * @param value
     *            The new value being aggregated into the cell
     */
    public ValueCellValue(final int count, final double aggregatedValue, final double minValue, final double maxValue) {
        if (count == 0 && (aggregatedValue != 0 || minValue != 0 || maxValue != 0))
            throw new IllegalArgumentException(
                    "Invalid arguments for constructor.  count is zero so aggregatedValue/minValue/maxValue cannot be non-zero");

        if (minValue > maxValue)
            throw new IllegalArgumentException(String.format(
                    "Invalid arguments for constructor.  minVlaue (%s) cannot be greater than maxValue(%s)", minValue,
                    maxValue));

        if (count == 1 && (minValue != aggregatedValue || maxValue != aggregatedValue)) {
            throw new IllegalArgumentException(String.format(
                    "Invalid arguments for constructor.  With a count of 1, minValue (%s) and maxValue (%s) must both equal the aggregatedValue (%s)",
                    minValue, maxValue, aggregatedValue));
        }

        this.count = count;
        this.aggregatedValue = aggregatedValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * Builds a {@link ValueCellValue} from just the value supplied. This method
     * initialises the object with a count of 1 so should only be used for an
     * initial value that has not been aggregated.
     *
     * @param value
     *            The new value being aggregated into the cell
     */
    public ValueCellValue(final double initialValue) {
        this.count = 1;
        this.aggregatedValue = initialValue;
        this.minValue = initialValue;
        this.maxValue = initialValue;
    }

    /**
     * Builds a {@link ValueCellValue} object from the bytes retrieved from a
     * hbase value cell.
     *
     * @param cellValue
     *            The byte[] from the cell
     */
    public ValueCellValue(final byte[] cellValue) {
        this(cellValue, 0, cellValue == null ? 0 : cellValue.length);
    }

    public ValueCellValue(final byte[] bytes, final int cellValueOffset, final int cellValueLength) {
        if (bytes == null || bytes.length == 0) {
            // cell doesn't exist or contains null so treat as zero value and
            // zero count
            this.count = 0;
            this.aggregatedValue = 0;
            this.minValue = 0;
            this.maxValue = 0;

        } else {
            // check the cell is the right length
            if (cellValueLength != CELL_BYTE_ARRAY_LENGTH) {
                throw new RuntimeException("Cell value is not a valid format.  Expecting " + CELL_BYTE_ARRAY_LENGTH
                        + ", got " + cellValueLength + ". Cell value is ["
                        + ByteArrayUtils.byteArrayToHex(Arrays.copyOfRange(bytes, cellValueOffset, cellValueLength)));
            }
            int position = cellValueOffset;

            // extract the four parts of the cell value (count, value, min and
            // max) from the byte[]
            this.count = Bytes.toInt(bytes, position, COUNT_BYTES_LENGTH);
            this.aggregatedValue = Bytes
                    .toDouble(Arrays.copyOfRange(bytes, position += COUNT_BYTES_LENGTH, position + VALUE_BYTES_LENGTH));
            this.minValue = Bytes
                    .toDouble(Arrays.copyOfRange(bytes, position += VALUE_BYTES_LENGTH, position + VALUE_BYTES_LENGTH));
            this.maxValue = Bytes
                    .toDouble(Arrays.copyOfRange(bytes, position += VALUE_BYTES_LENGTH, position + VALUE_BYTES_LENGTH));
        }
    }

    /**
     * Method to construct a new {@link ValueCellValue} object by adding a
     * double value to the existing value and incrementing the count.
     *
     * @param value
     *            The new value to add to the cell
     * @return A new {@link ValueCellValue} object
     */
    public ValueCellValue addValue(final double value) {
        return new ValueCellValue(this.count + 1, this.aggregatedValue + value, min(this.minValue, value),
                max(this.maxValue, value));
    }

    /**
     * Method to return a new {@link ValueCellValue} instance based on the
     * aggregation of the values in another cell value and this one. The counts
     * from each cell are added together and the values from each cell are added
     * together
     *
     * @param other
     *            The cell value object to add to the values in this one
     * @return A new {@link ValueCellValue} object containing the aggregate of
     *         both cell values
     */
    public ValueCellValue addAggregatedValues(final ValueCellValue other) {
        if (this.count == 0 && other.count != 0) {
            // this has zero count so has never been written to so can use all
            // values from other
            return new ValueCellValue(other.count, other.aggregatedValue, other.minValue, other.maxValue);
        } else if (this.count != 0 && other.count == 0) {
            // other has zero count so has never been written to so can use all
            // values from this
            return new ValueCellValue(this.count, this.aggregatedValue, this.minValue, this.maxValue);
        } else if (this.count == 0 && other.count == 0) {
            // both are empty so just return one of them, might as well be this.
            return this;
        } else {
            return new ValueCellValue(this.count + other.count, this.aggregatedValue + other.aggregatedValue,
                    min(this.minValue, other.minValue), max(this.maxValue, other.maxValue));
        }
    }

    /**
     * Convert the count and value into byte arrays and concatenate the two
     * arrays together into one 12 byte array.
     *
     * @return A 12 byte array containing the count (4 bytes), value (8 bytes),
     *         min ( in that order
     */
    public byte[] asByteArray() {
        final byte[] arr = new byte[CELL_BYTE_ARRAY_LENGTH];
        final ByteBuffer buffer = ByteBuffer.wrap(arr);
        buffer.put(Bytes.toBytes(this.count));
        buffer.put(Bytes.toBytes(this.aggregatedValue));
        buffer.put(Bytes.toBytes(this.minValue));
        buffer.put(Bytes.toBytes(this.maxValue));

        return buffer.array();
    }

    public int getCount() {
        return count;
    }

    /**
     * @return The sum of all values that have been aggregated into the cell.
     *         E.g if the statistic value is cpu% then it may be formed from two
     *         values (80% and 90%) so this method will return 170. 170 has no
     *         meaning without the count. If you want an average of the values
     *         then use getAverageValue() instead.
     */
    public double getAggregatedValue() {
        return aggregatedValue;
    }

    /**
     * @return The minimum value that has been aggregated into this cell
     */
    public double getMinValue() {
        return minValue;
    }

    /**
     * @return The maximum value that has been aggregated into this cell
     */
    public double getMaxValue() {
        return maxValue;
    }

    /**
     * Method to obtain the average value for the cell, based on the
     * aggregatedValue and the count. E.g if the statistic value is cpu% then it
     * may be formed from two values (80% and 90%) so this method will return
     * 170/2, i.e. 85.
     *
     * @return An average (mean) value for all values aggregated into this cell
     */
    public double getAverageValue() {
        if (count == 0) {
            return 0;
        } else if (count == 1) {
            return aggregatedValue;
        } else {
            return aggregatedValue / count;
        }
    }

    public boolean isEmpty() {
        return (count == 0 && aggregatedValue == 0);
    }

    private double min(final double val1, final double val2) {
        return val2 > val1 ? val1 : val2;
    }

    private double max(final double val1, final double val2) {
        return val2 < val1 ? val1 : val2;
    }

    @Override
    public String toString() {
        return "ValueCellValue [count=" + count + ", aggregatedValue=" + aggregatedValue + ", minValue=" + minValue
                + ", maxValue=" + maxValue + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(aggregatedValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + count;
        temp = Double.doubleToLongBits(maxValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
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
        final ValueCellValue other = (ValueCellValue) obj;
        if (Double.doubleToLongBits(aggregatedValue) != Double.doubleToLongBits(other.aggregatedValue))
            return false;
        if (count != other.count)
            return false;
        if (Double.doubleToLongBits(maxValue) != Double.doubleToLongBits(other.maxValue))
            return false;
        if (Double.doubleToLongBits(minValue) != Double.doubleToLongBits(other.minValue))
            return false;
        return true;
    }
}
