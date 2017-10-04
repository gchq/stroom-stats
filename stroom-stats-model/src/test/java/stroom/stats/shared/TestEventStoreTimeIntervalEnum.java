

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

package stroom.stats.shared;

import com.google.common.primitives.Bytes;
import javaslang.Tuple2;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.util.DateUtil;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestEventStoreTimeIntervalEnum {
    @Test
    public void testFromColumnInterval() throws Exception {
        for (final EventStoreTimeIntervalEnum intervalEnum : EventStoreTimeIntervalEnum.values()) {
            assertEquals(intervalEnum,
                    EventStoreTimeIntervalEnum.fromColumnInterval(intervalEnum.columnInterval()));
        }
    }

    @Test
    public void testFromShortName() throws Exception {
        for (final EventStoreTimeIntervalEnum intervalEnum : EventStoreTimeIntervalEnum.values()) {
            assertEquals(intervalEnum, EventStoreTimeIntervalEnum.fromShortName(intervalEnum.shortName()));
        }
    }

    @Test
    public void testTruncteTimeToColumnInterval() throws Exception {
        Arrays.stream(EventStoreTimeIntervalEnum.values())
                .filter(interval -> !interval.equals(EventStoreTimeIntervalEnum.FOREVER))
                .forEach(interval -> {
                    final long timeMs = (interval.columnInterval() * 2) + 1;
                    final long expectedTimeMs = (interval.columnInterval() * 2);

                    final long roundedTime = interval.truncateTimeToColumnInterval(timeMs);

                    assertEquals(expectedTimeMs, roundedTime);
                });
    }

    @Test
    public void testTruncteTimeToColumnInterval_specific() {
        final String eventTimeString = "2009-01-01T10:11:12.134Z";
        final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        long eventTimeRounded = EventStoreTimeIntervalEnum.SECOND.truncateTimeToColumnInterval(eventTime);

        assertEquals("2009-01-01T10:11:12.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));

        eventTimeRounded = EventStoreTimeIntervalEnum.MINUTE.truncateTimeToColumnInterval(eventTime);

        assertEquals("2009-01-01T10:11:00.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));

        eventTimeRounded = EventStoreTimeIntervalEnum.HOUR.truncateTimeToColumnInterval(eventTime);

        assertEquals("2009-01-01T10:00:00.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));

        eventTimeRounded = EventStoreTimeIntervalEnum.DAY.truncateTimeToColumnInterval(eventTime);

        assertEquals("2009-01-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));

        eventTimeRounded = EventStoreTimeIntervalEnum.FOREVER.truncateTimeToColumnInterval(eventTime);

        assertEquals("1970-01-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(eventTimeRounded));
    }

    @Test
    public void testTruncteTimeToColumnInterval_specific2() throws Exception {

        List<Tuple2<EventStoreTimeIntervalEnum, ChronoUnit>> pairs = new ArrayList<>();
        pairs.add(new Tuple2<>(EventStoreTimeIntervalEnum.SECOND, ChronoUnit.SECONDS));
        pairs.add(new Tuple2<>(EventStoreTimeIntervalEnum.MINUTE, ChronoUnit.MINUTES));
        pairs.add(new Tuple2<>(EventStoreTimeIntervalEnum.HOUR, ChronoUnit.HOURS));
        pairs.add(new Tuple2<>(EventStoreTimeIntervalEnum.DAY, ChronoUnit.DAYS));

        LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40, 999);
        long timeMS = time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        pairs.forEach(pair -> {

            Assertions.assertThat(pair._1().truncateTimeToColumnInterval(timeMS))
                    .isEqualTo(time.atZone(ZoneOffset.UTC).toInstant().truncatedTo(pair._2()).toEpochMilli());
        });

        //special case for FOREVER
        Assertions.assertThat(EventStoreTimeIntervalEnum.FOREVER.truncateTimeToColumnInterval(timeMS))
                .isEqualTo(0);

    }

    @Test
    public void testRoundTimeToRowKeyInterval() throws Exception {
        Arrays.stream(EventStoreTimeIntervalEnum.values())
                .filter(interval -> !interval.equals(EventStoreTimeIntervalEnum.FOREVER))
                .forEach(interval -> {
                    final long timeMs = (interval.rowKeyInterval() * 2) + 1;
                    final long expectedTimeMs = (interval.rowKeyInterval() * 2);

                    final long roundedTime = interval.truncateTimeToRowKeyInterval(timeMs);

                    assertEquals(expectedTimeMs, roundedTime);
                });
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            byte[] bytes = interval.getByteVal();
            EventStoreTimeIntervalEnum interval2 = EventStoreTimeIntervalEnum.fromBytes(bytes);
            Assertions.assertThat(interval).isEqualTo(interval2);
        }
    }

    /**
     * Deserialize from a bigger byte array where the interval is just part of it.
     */
    @Test
    public void testSerializationDeserialization2() throws Exception {
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            byte[] bytes = interval.getByteVal();
            int paddingSize = 10;
            byte[] padding = new byte[paddingSize];
            byte[] bigByteArray = Bytes.concat(padding, bytes);

            EventStoreTimeIntervalEnum interval2 = EventStoreTimeIntervalEnum.fromBytes(bigByteArray, paddingSize);
            Assertions.assertThat(interval).isEqualTo(interval2);
        }
    }
}
