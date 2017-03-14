

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

package stroom.stats.shared;

import com.google.common.primitives.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class TestEventStoreTimeIntervalEnum {
    @Test
    public void testFromColumnInterval() throws Exception {
        for (final EventStoreTimeIntervalEnum intervalEnum : EventStoreTimeIntervalEnum.values()) {
            Assert.assertEquals(intervalEnum,
                    EventStoreTimeIntervalEnum.fromColumnInterval(intervalEnum.columnInterval()));
        }
    }

    @Test
    public void testFromShortName() throws Exception {
        for (final EventStoreTimeIntervalEnum intervalEnum : EventStoreTimeIntervalEnum.values()) {
            Assert.assertEquals(intervalEnum, EventStoreTimeIntervalEnum.fromShortName(intervalEnum.shortName()));
        }
    }

    @Test
    public void testRoundTimeToColumnInterval() throws Exception {
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            final long timeMs = (interval.columnInterval() * 2) + 1;
            final long expectedTimeMs = (interval.columnInterval() * 2);

            final long roundedTime = interval.roundTimeToColumnInterval(timeMs);

            Assert.assertEquals(expectedTimeMs, roundedTime);
        }
    }

    @Test
    public void testRoundTimeToColumnInterval_specific() throws Exception {
        LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40, 999);
        long timeMS = time.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();

        Assertions.assertThat(EventStoreTimeIntervalEnum.SECOND.roundTimeToColumnInterval(timeMS))
                .isEqualTo(time.atZone(ZoneOffset.UTC).toInstant().truncatedTo(ChronoUnit.SECONDS).toEpochMilli());
    }

    @Test
    public void testRoundTimeToRowKeyInterval() throws Exception {
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            final long timeMs = (interval.rowKeyInterval() * 2) + 1;
            final long expectedTimeMs = (interval.rowKeyInterval() * 2);

            final long roundedTime = interval.roundTimeToRowKeyInterval(timeMs);

            Assert.assertEquals(expectedTimeMs, roundedTime);
        }
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
