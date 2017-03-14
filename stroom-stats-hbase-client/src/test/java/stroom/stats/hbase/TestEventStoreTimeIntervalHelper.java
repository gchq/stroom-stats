

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

package stroom.stats.hbase;

import org.junit.Test;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEventStoreTimeIntervalHelper {
    @Test
    public void testGetMatchingIntervalsExactMatch() {
        final long exactMatchVal = 3_600_000L;

        final List<EventStoreTimeIntervalEnum> matchList = EventStoreTimeIntervalHelper
                .getMatchingIntervals(exactMatchVal);

        assertEquals(2, matchList.size());
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.HOUR));
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.DAY));
    }

    @Test
    public void testGetMatchingIntervalsTooSmall() {
        final long exactMatchVal = 1L;

        final List<EventStoreTimeIntervalEnum> matchList = EventStoreTimeIntervalHelper
                .getMatchingIntervals(exactMatchVal);

        assertEquals(4, matchList.size());
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.SECOND));
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.MINUTE));
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.HOUR));
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.DAY));

    }

    @Test
    public void testGetMatchingIntervalsBetweenTwo() {
        final long exactMatchVal = 63_000L;

        final List<EventStoreTimeIntervalEnum> matchList = EventStoreTimeIntervalHelper
                .getMatchingIntervals(exactMatchVal);

        assertEquals(3, matchList.size());
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.MINUTE));
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.HOUR));
        assertTrue(matchList.contains(EventStoreTimeIntervalEnum.DAY));
    }

    @Test(expected = RuntimeException.class)
    public void testGetMatchingIntervalsTooBig() {
        final long exactMatchVal = Long.MAX_VALUE;

        EventStoreTimeIntervalHelper.getMatchingIntervals(exactMatchVal);

    }

    @Test
    public void testGetNextBiggest() {
        EventStoreTimeIntervalEnum currentInterval = EventStoreTimeIntervalEnum.MINUTE;

        assertEquals(EventStoreTimeIntervalEnum.HOUR, EventStoreTimeIntervalHelper.getNextBiggest(currentInterval).get());

        currentInterval = EventStoreTimeIntervalEnum.DAY;

        assertEquals(Optional.empty(), EventStoreTimeIntervalHelper.getNextBiggest(currentInterval));

        currentInterval = EventStoreTimeIntervalEnum.SECOND;

        assertEquals(EventStoreTimeIntervalEnum.MINUTE, EventStoreTimeIntervalHelper.getNextBiggest(currentInterval).get());
    }

    @Test
    public void testGetSmallest() {
        assertEquals(EventStoreTimeIntervalEnum.SECOND, EventStoreTimeIntervalHelper.getSmallestInterval());
    }

    @Test
    public void testGetLargest() {
        assertEquals(EventStoreTimeIntervalEnum.DAY, EventStoreTimeIntervalHelper.getLargestInterval());
    }

    @Test
    public void testRoundTimeToColumnInterval() {
        final String eventTimeString = "2009-01-01T10:11:12.134Z";
        final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        long eventTimeRounded = EventStoreTimeIntervalEnum.SECOND.roundTimeToColumnInterval(eventTime);

        assertEquals("2009-01-01T10:11:12.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));

        eventTimeRounded = EventStoreTimeIntervalEnum.HOUR.roundTimeToColumnInterval(eventTime);

        assertEquals("2009-01-01T10:00:00.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));
    }

    @Test
    public void testGetBestFitByPeriodAndMaxIntervals() {
        // 40mins
        long period = 40 * 60 * 1000;
        final int desiredMaxIntervals = 100;

        EventStoreTimeIntervalEnum bestFitInterval = EventStoreTimeIntervalHelper.getBestFit(period,
                desiredMaxIntervals);

        assertEquals(EventStoreTimeIntervalEnum.MINUTE, bestFitInterval);

        period = 60 * 1000 * desiredMaxIntervals;

        bestFitInterval = EventStoreTimeIntervalHelper.getBestFit(period, desiredMaxIntervals);

        assertEquals(EventStoreTimeIntervalEnum.MINUTE, bestFitInterval);

        period = Long.MAX_VALUE;

        bestFitInterval = EventStoreTimeIntervalHelper.getBestFit(period, desiredMaxIntervals);

        assertEquals(EventStoreTimeIntervalHelper.getLargestInterval(), bestFitInterval);

        period = 1L;

        bestFitInterval = EventStoreTimeIntervalHelper.getBestFit(period, desiredMaxIntervals);

        assertEquals(EventStoreTimeIntervalHelper.getSmallestInterval(), bestFitInterval);
    }
}
