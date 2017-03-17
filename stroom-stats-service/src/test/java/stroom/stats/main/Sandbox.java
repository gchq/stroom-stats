

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

package stroom.stats.main;

import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;

import java.util.Arrays;
import java.util.Random;

public class Sandbox {
    /**
     * @param args
     */
    public static void main(final String[] args) {
        final long time = 137;
        final long interval = 20;

        final long intervalNo = time / interval;
        final long remainder = time % interval;

        System.out.println("intervalNo: " + intervalNo);
        System.out.println("remainder: " + remainder);

        String val = "";
        val = "000000";
        System.out.println(val + ": " + val);

        val = "000001";
        System.out.println(val + ": " + Bytes.toHex(Bytes.toBytes(val)));

        val = "999999";
        System.out.println(val + ": " + Bytes.toHex(Bytes.toBytes(val)));

        val = "999999";
        System.out.println(val + ": " + toByteString(Bytes.toBytes(val)));

        int i = 1111111;
        System.out.println(i + ": " + Bytes.toHex(Bytes.toBytes(i)));

        i = 1111111;
        System.out.println(i + ": " + toByteString(Bytes.toBytes(i)));

        i = 1;
        System.out.println(i + ": " + toByteString(Bytes.toBytes(i)));

        final long l = 99999999999999L;
        System.out.println(l + ": " + toByteString(Bytes.toBytes(l)));

        i = 0;
        System.out.println(i + ": " + toByteString(Bytes.toBytes(i)));

        i = 3600;
        System.out.println(i + ": " + toByteString(Bytes.toBytes(i)));

        double d = 1234567890d;
        System.out.println(d + ": " + d);

        d = 1.23456789;
        System.out.println(d + ": " + d);

        final int[] arr = { 10, 20, 30, 40, 50, 60 };
        System.out.println("length: " + arr.length);

        System.out.println("1st half: " + dumpArray(Arrays.copyOf(arr, 3)));
        System.out.println("1st half: " + dumpArray(Arrays.copyOfRange(arr, 3, arr.length)));

        System.out.println(Bytes.toInt(Bytes.toBytes(12345)));

        final int i2 = 1234567;
        final byte[] b2 = Bytes.toBytes(i2);
        System.out.println(byteArrayToString(b2));

        System.out.println(Bytes.toInt(b2));

        final byte[] b3 = { 0, 5, 89, -80 };
        System.out.println(Bytes.toInt(b3));

        final long l1 = 350_640;
        final long l2 = 3_600_000;
        final long l3 = l1 * l2;
        System.out.println(l3);

        final long l4 = 162516258121L;
        final double d4 = l4;
        final long l5 = (long) d4;
        System.out.println(l5);

        System.out.println(ByteArrayUtils.byteArrayToHex(Bytes.toBytes(Double.valueOf(1))));

        System.out.println(new Random().nextDouble() * 100);

    }

    private static String dumpArray(final int[] arr) {
        String retVal = "";
        for (final int i : arr) {
            retVal += i + ",";
        }
        return retVal;
    }

    private static String byteArrayToString(final byte[] arr) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : arr) {
            sb.append(b);
            sb.append(" ");
        }
        final String retVal = sb.toString().replaceAll(" $", "");

        return "[" + retVal + "]";
    }

    private static String toByteString(final byte[] byteVal) {
        String retVal = "[";

        for (final byte b : byteVal) {
            retVal += Byte.toString(b) + " ";
        }

        retVal += "]";

        return retVal;
    }
}
