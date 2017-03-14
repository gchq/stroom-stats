

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

package stroom.stats.util;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;


public class TestDateUtil {
    @Test
    public void testParseManualTimeZones() throws ParseException {
        long date = -1;

        date = DateUtil.parseDate("yyyy/MM/dd", "-07:00", "2001/08/01");
        Assert.assertEquals("2001-08-01T07:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd HH:mm:ss", "-08:00", "2001/08/01 01:00:00");
        Assert.assertEquals("2001-08-01T09:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd HH:mm:ss", "+01:00", "2001/08/01 01:00:00");
        Assert.assertEquals("2001-08-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(date));
    }

    @Test
    public void testParse() throws ParseException {
        long date = -1;

        date = DateUtil.parseDate("yyyy/MM/dd", null, "2001/01/01");
        Assert.assertEquals("2001-01-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd", "GMT", "2001/08/01");
        Assert.assertEquals("2001-08-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd HH:mm:ss.SSS", "GMT", "2001/08/01 00:00:00.000");
        Assert.assertEquals("2001-08-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd HH:mm:ss", "Europe/London", "2001/08/01 00:00:00");
        Assert.assertEquals("2001-07-31T23:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd", "GMT", "2001/01/01");
        Assert.assertEquals("2001-01-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd:HH:mm:ss", "Europe/London", "2008/08/08:00:00:00");
        Assert.assertEquals("2008-08-07T23:00:00.000Z", DateUtil.createNormalDateTimeString(date));

        date = DateUtil.parseDate("yyyy/MM/dd", "Europe/London", "2008/08/08");
        Assert.assertEquals("2008-08-07T23:00:00.000Z", DateUtil.createNormalDateTimeString(date));
    }

    @Test
//    @StroomExpectedException(exception = IllegalArgumentException.class)
    public void testParseGMTBSTGuess() {
        // Null
        boolean thrownException = false;
        try {
            doGMTBSTGuessTest(null, "");
        } catch (final IllegalArgumentException e) {
            thrownException = true;
        }
        Assert.assertTrue(thrownException);

        // Winter
        doGMTBSTGuessTest("2011-01-01T00:00:00.999Z", "2011/01/01 00:00:00.999");

        // MID Point Summer Time 1 Aug
        doGMTBSTGuessTest("2001-08-01T03:00:00.000Z", "2001/08/01 04:00:00.000");
        doGMTBSTGuessTest("2011-08-01T03:00:00.000Z", "2011/08/01 04:00:00.000");

        // Boundary WINTER TO SUMMER
        doGMTBSTGuessTest("2011-03-26T22:59:59.999Z", "2011/03/26 22:59:59.999");
        doGMTBSTGuessTest("2011-03-26T23:59:59.999Z", "2011/03/26 23:59:59.999");
        doGMTBSTGuessTest("2011-03-27T00:00:00.000Z", "2011/03/27 00:00:00.000");
        doGMTBSTGuessTest("2011-03-27T00:59:59.000Z", "2011/03/27 00:59:59.000");
        // Lost an hour!
        doGMTBSTGuessTest("2011-03-27T00:00:00.000Z", "2011/03/27 00:00:00.000");
        doGMTBSTGuessTest("2011-03-27T01:59:00.999Z", "2011/03/27 01:59:00.999");
        doGMTBSTGuessTest("2011-03-27T02:00:00.999Z", "2011/03/27 03:00:00.999");

        // Boundary SUMMER TO WINTER
        doGMTBSTGuessTest("2011-10-29T23:59:59.999Z", "2011/10/30 00:59:59.999");
    }

    private void doGMTBSTGuessTest(final String expected, final String parse) {
        final long date = DateUtil.parseDate("yyyy/MM/dd HH:mm:ss.SSS", "GMT/BST", parse);
        Assert.assertEquals(expected, DateUtil.createNormalDateTimeString(date));
    }

    @Test
    public void testSimpleZuluTimes() {
        doTest("2008-11-18T09:47:50.548Z");
        doTest("2008-11-18T09:47:00.000Z");
        doTest("2008-11-18T13:47:00.000Z");
        doTest("2008-01-01T13:47:00.000Z");
        doTest("2008-08-01T13:47:00.000Z");
    }

    private void doTest(final String dateString) {
        final long date = DateUtil.parseNormalDateTimeString(dateString);

        // Convert Back to string
        Assert.assertEquals(dateString, DateUtil.createNormalDateTimeString(date));
    }

    @Test
    public void testSimple() {
        Assert.assertEquals("2010-01-01T23:59:59.000Z",
                DateUtil.createNormalDateTimeString(DateUtil.parseNormalDateTimeString("2010-01-01T23:59:59.000Z")));

    }

    @Test
    public void testSimpleFileFormat() {
        Assert.assertEquals("2010-01-01T23#59#59,000Z",
                DateUtil.createFileDateTimeString(DateUtil.parseNormalDateTimeString("2010-01-01T23:59:59.000Z")));
    }
}
