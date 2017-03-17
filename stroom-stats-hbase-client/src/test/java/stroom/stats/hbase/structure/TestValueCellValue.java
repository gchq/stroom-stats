

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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;


public class TestValueCellValue {
    private static final double JUNIT_DOUBLE_COMPARE_DELTA = 0.0001d;

    @Test
    public void test_toBytesAndBack() {
        final ValueCellValue valueCellValue = new ValueCellValue(3, 1.5, 0.5, 2);

        final byte[] bytes = valueCellValue.asByteArray();

        final ValueCellValue valueCellValueNew = new ValueCellValue(bytes);

        assertEquals("Objects don't match", valueCellValue.toString(), valueCellValueNew.toString());

    }

    @Test
    public void test_addValue() {
        final ValueCellValue valueCellValue = new ValueCellValue(1, 1.5, 1.5, 1.5);

        Assert.assertEquals(1, valueCellValue.getCount());

        final ValueCellValue valueCellValueNew = valueCellValue.addValue(0.25);

        Assert.assertEquals(1.5 + 0.25, valueCellValueNew.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(0.25, valueCellValueNew.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(1.5, valueCellValueNew.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);

        Assert.assertEquals(2, valueCellValueNew.getCount());

        Assert.assertEquals((1.5 + 0.25) / 2, valueCellValueNew.getAverageValue(), JUNIT_DOUBLE_COMPARE_DELTA);
    }

    @Test
    public void test_isEmpty() {
        ValueCellValue valueCellValue = new ValueCellValue(0, 0, 0, 0);

        assertEquals(true, valueCellValue.isEmpty());

        valueCellValue = new ValueCellValue(1, 0, 0, 0);

        assertEquals(false, valueCellValue.isEmpty());

    }

    @Test(expected = IllegalArgumentException.class)
    public void test_valueCellValue_InvalidValues() {
        new ValueCellValue(0, 1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_valueCellValue_MinBiggerThanMax() {
        new ValueCellValue(0, 10, 10, 1);
    }

    @Test(expected = RuntimeException.class)
    public void test_valueCellValue_InvalidBytes() {
        // byte array is too short
        new ValueCellValue(new byte[] { 0, 0 });
    }

    @Test
    public void test_addTwoValueCellValues() {
        final ValueCellValue valueCellValue1 = new ValueCellValue(3, 5, 1, 3);
        final ValueCellValue valueCellValue2 = new ValueCellValue(3, 5, 2, 4);

        final ValueCellValue valueCellValue3 = valueCellValue1.addAggregatedValues(valueCellValue2);

        Assert.assertEquals(10, valueCellValue3.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(1, valueCellValue3.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(4, valueCellValue3.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);

        Assert.assertEquals(6, valueCellValue3.getCount());

    }

    @Test
    public void test_addTwoValueCellValuesOneIsZero1() {
        final ValueCellValue valueCellValue1 = ValueCellValue.emptyInstance();
        final ValueCellValue valueCellValue2 = new ValueCellValue(3, 5, 2, 4);

        final ValueCellValue valueCellValue3 = valueCellValue1.addAggregatedValues(valueCellValue2);

        Assert.assertEquals(5, valueCellValue3.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(2, valueCellValue3.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(4, valueCellValue3.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);

        Assert.assertEquals(3, valueCellValue3.getCount());

    }

    @Test
    public void test_addTwoValueCellValuesOneIsZero2() {
        final ValueCellValue valueCellValue1 = new ValueCellValue(3, 5, 1, 3);
        final ValueCellValue valueCellValue2 = ValueCellValue.emptyInstance();

        final ValueCellValue valueCellValue3 = valueCellValue1.addAggregatedValues(valueCellValue2);

        Assert.assertEquals(5, valueCellValue3.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(1, valueCellValue3.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(3, valueCellValue3.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);

        Assert.assertEquals(3, valueCellValue3.getCount());

    }

    @Test
    public void test_addTwoValueCellValuesBothZero() {
        final ValueCellValue valueCellValue1 = ValueCellValue.emptyInstance();
        final ValueCellValue valueCellValue2 = ValueCellValue.emptyInstance();

        final ValueCellValue valueCellValue3 = valueCellValue1.addAggregatedValues(valueCellValue2);

        Assert.assertEquals(valueCellValue3, valueCellValue1);
        Assert.assertEquals(valueCellValue3, valueCellValue2);

        Assert.assertEquals(0, valueCellValue3.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(0, valueCellValue3.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(0, valueCellValue3.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);

        Assert.assertEquals(0, valueCellValue3.getCount());

    }

    @Test
    public void test_valueCellValue() {
        final ValueCellValue valueCellValue = new ValueCellValue(2, 3.6, 1.5, 3.6);

        assertEquals(2, valueCellValue.getCount());
        assertEquals(3.6, valueCellValue.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(1.5, valueCellValue.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(3.6, valueCellValue.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);
    }

    @Test
    public void test_valueCellValue2() {
        final ValueCellValue valueCellValue = new ValueCellValue(3.6);

        assertEquals(1, valueCellValue.getCount());
        assertEquals(3.6, valueCellValue.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(3.6, valueCellValue.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(3.6, valueCellValue.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);
    }

    @Test
    public void test_addManyValues() {
        final double valToAdd = 0.1;
        final int iterations = 400_000;

        ValueCellValue valueCellValue = new ValueCellValue(1, valToAdd, valToAdd, valToAdd);

        for (int i = 2; i <= iterations; i++) {
            final ValueCellValue valueCellValueNew = valueCellValue.addValue(valToAdd);
            valueCellValue = valueCellValueNew;
        }

        Assert.assertEquals(iterations * valToAdd, valueCellValue.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(valToAdd, valueCellValue.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(valToAdd, valueCellValue.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);

        System.out.println(valueCellValue.getAggregatedValue());

        Assert.assertEquals(iterations, valueCellValue.getCount());

    }

    @Test
    public void constructorFromBytes() {
        final byte[] bCount = Bytes.toBytes(2);
        final byte[] bValue = Bytes.toBytes(new Double(10));
        final byte[] bMin = Bytes.toBytes(new Double(2));
        final byte[] bMax = Bytes.toBytes(new Double(8));

        final byte[] bcellValue = new byte[bCount.length + bValue.length + bMin.length + bMax.length];

        final ByteBuffer byteBuffer = ByteBuffer.wrap(bcellValue);

        byteBuffer.put(bCount);
        byteBuffer.put(bValue);
        byteBuffer.put(bMin);
        byteBuffer.put(bMax);

        final ValueCellValue celValue = new ValueCellValue(bcellValue);

        Assert.assertEquals(2, celValue.getCount());
        Assert.assertEquals(10, celValue.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(2, celValue.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(8, celValue.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals((8 + 2) / 2, celValue.getAverageValue(), JUNIT_DOUBLE_COMPARE_DELTA);
    }

    @Test
    public void constructorFromBytesNullArray() {
        final byte[] arr = null;

        final ValueCellValue cellValue = new ValueCellValue(arr);

        Assert.assertEquals(ValueCellValue.emptyInstance(), cellValue);
    }

    @Test
    public void constructorFromBytesZeroLengthArray() {
        final byte[] arr = new byte[0];

        final ValueCellValue cellValue = new ValueCellValue(arr);

        Assert.assertEquals(ValueCellValue.emptyInstance(), cellValue);
    }

    @Test
    public void constructorFromBytesWithOffsetAndLength() {
        final byte[] bRandomBytes = Bytes.toBytes(new Double(99));
        final byte[] bCount = Bytes.toBytes(2);
        final byte[] bValue = Bytes.toBytes(new Double(10));
        final byte[] bMin = Bytes.toBytes(new Double(2));
        final byte[] bMax = Bytes.toBytes(new Double(8));

        final byte[] bcellValue = new byte[bRandomBytes.length + ValueCellValue.CELL_BYTE_ARRAY_LENGTH
                + bRandomBytes.length];

        final ByteBuffer byteBuffer = ByteBuffer.wrap(bcellValue);

        byteBuffer.put(bRandomBytes);
        byteBuffer.put(bCount);
        byteBuffer.put(bValue);
        byteBuffer.put(bMin);
        byteBuffer.put(bMax);
        byteBuffer.put(bRandomBytes);

        final ValueCellValue celValue = new ValueCellValue(bcellValue, bRandomBytes.length,
                ValueCellValue.CELL_BYTE_ARRAY_LENGTH);

        Assert.assertEquals(2, celValue.getCount());
        Assert.assertEquals(10, celValue.getAggregatedValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(2, celValue.getMinValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals(8, celValue.getMaxValue(), JUNIT_DOUBLE_COMPARE_DELTA);
        Assert.assertEquals((8 + 2) / 2, celValue.getAverageValue(), JUNIT_DOUBLE_COMPARE_DELTA);
    }

    @Test
    public void constructorFromBytesWithOffsetAndLengthNullArray() {
        final byte[] arr = null;

        final ValueCellValue cellValue = new ValueCellValue(arr, 0, ValueCellValue.CELL_BYTE_ARRAY_LENGTH);

        Assert.assertEquals(ValueCellValue.emptyInstance(), cellValue);
    }

    @Test
    public void constructorFromBytesWithOffsetAndLengthZeroLengthArray() {
        final byte[] arr = new byte[0];

        final ValueCellValue cellValue = new ValueCellValue(arr, 0, ValueCellValue.CELL_BYTE_ARRAY_LENGTH);

        Assert.assertEquals(ValueCellValue.emptyInstance(), cellValue);
    }

    @Test(expected = RuntimeException.class)
    public void constructorFromBytesWithOffsetAndLengthInvalidLength() {
        final byte[] bRandomBytes = Bytes.toBytes(new Double(99));
        final byte[] bCount = Bytes.toBytes(2);
        final byte[] bValue = Bytes.toBytes(new Double(10));
        final byte[] bMin = Bytes.toBytes(new Double(2));
        final byte[] bMax = Bytes.toBytes(new Double(8));

        final byte[] bcellValue = new byte[bRandomBytes.length + ValueCellValue.CELL_BYTE_ARRAY_LENGTH
                + bRandomBytes.length];

        final ByteBuffer byteBuffer = ByteBuffer.wrap(bcellValue);

        byteBuffer.put(bRandomBytes);
        byteBuffer.put(bCount);
        byteBuffer.put(bValue);
        byteBuffer.put(bMin);
        byteBuffer.put(bMax);
        byteBuffer.put(bRandomBytes);

        // expecting exception here
        new ValueCellValue(bcellValue, bRandomBytes.length, ValueCellValue.CELL_BYTE_ARRAY_LENGTH - 1);
    }
}
