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

package stroom.stats.streams;

import org.apache.hadoop.hbase.util.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.uid.UID;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestStatKey {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStatKey.class);

    UID statNameUid = UID.from(new byte[] {9,0,0,1});
    RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
    EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
    LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40);

    private StatKey buildStatKey() {
        long timeMs = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        List<TagValue> tagValues = new ArrayList<>();
        tagValues.add(new TagValue(UID.from(new byte[] {9,0,1,1}), UID.from(new byte[] {9,0,1,1})));
        tagValues.add(new TagValue(UID.from(new byte[] {9,0,2,1}), UID.from(new byte[] {9,0,2,2})));
        tagValues.add(new TagValue(UID.from(new byte[] {9,0,3,1}), UID.from(new byte[] {9,0,3,2})));

        StatKey statKey = new StatKey(statNameUid, rollUpBitMask, interval, timeMs, tagValues);

        //make sure the time in the statkey has been truncated to the interval of the statkey
        Assertions.assertThat(statKey.getTimeMs()).isEqualTo(Instant.ofEpochMilli(timeMs).truncatedTo(ChronoUnit.MINUTES).toEpochMilli());

        return statKey;
    }


    @Test
    public void cloneAndChangeInterval() throws Exception {

        StatKey statKey = buildStatKey();


        EventStoreTimeIntervalEnum expectedInterval = EventStoreTimeIntervalEnum.DAY;

        StatKey statKey2 = statKey.cloneAndChangeInterval(expectedInterval);

        Assertions.assertThat(statKey2.getInterval()).isEqualTo(expectedInterval);
        Assertions.assertThat(statKey2.getInterval()).isNotEqualTo(statKey.getInterval());

        Assertions.assertThat(statKey2.getTimeMs()).isNotEqualTo(statKey.getTimeMs());
        Assertions.assertThat(statKey2.getTimeMs()).isEqualTo(time.toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).toEpochMilli());

        //Tags unchanged
        Assertions.assertThat(statKey2.getTagValues().get(0).getTag()).isEqualTo(statKey.getTagValues().get(0).getTag());
        Assertions.assertThat(statKey2.getTagValues().get(1).getTag()).isEqualTo(statKey.getTagValues().get(1).getTag());
        Assertions.assertThat(statKey2.getTagValues().get(2).getTag()).isEqualTo(statKey.getTagValues().get(2).getTag());

        //values unchanged
        Assertions.assertThat(statKey2.getTagValues().get(0).getValue()).isEqualTo(statKey.getTagValues().get(0).getValue());
        Assertions.assertThat(statKey2.getTagValues().get(1).getValue()).isEqualTo(statKey.getTagValues().get(1).getValue());
        Assertions.assertThat(statKey2.getTagValues().get(2).getValue()).isEqualTo(statKey.getTagValues().get(2).getValue());

        //other props unchanged
        Assertions.assertThat(statKey2.getStatName()).isEqualTo(statKey.getStatName());
        Assertions.assertThat(statKey2.getRollupMask()).isEqualTo(statKey.getRollupMask());

    }

    @Test
    public void cloneAndRollUpTags() throws Exception {

        StatKey statKey = buildStatKey();

        RollUpBitMask newRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(0,2));
        UID rolledUpValue = UID.from(new byte[] {8,8,8,8});

        StatKey statKey2 = statKey.cloneAndRollUpTags(newRollUpBitMask, rolledUpValue);

        //Tags unchanged
        Assertions.assertThat(statKey2.getTagValues().get(0).getTag()).isEqualTo(statKey.getTagValues().get(0).getTag());
        Assertions.assertThat(statKey2.getTagValues().get(1).getTag()).isEqualTo(statKey.getTagValues().get(1).getTag());
        Assertions.assertThat(statKey2.getTagValues().get(2).getTag()).isEqualTo(statKey.getTagValues().get(2).getTag());

        //other props unchanged
        Assertions.assertThat(statKey2.getStatName()).isEqualTo(statKey.getStatName());
        Assertions.assertThat(statKey2.getInterval()).isEqualTo(statKey.getInterval());
        Assertions.assertThat(statKey2.getTimeMs()).isEqualTo(statKey.getTimeMs());

        //tagValues 0 and 2 rolled up
        Assertions.assertThat(statKey2.getTagValues().get(0).getValue()).isEqualTo(rolledUpValue);
        Assertions.assertThat(statKey2.getTagValues().get(1).getValue()).isEqualTo(statKey.getTagValues().get(1).getValue());
        Assertions.assertThat(statKey2.getTagValues().get(2).getValue()).isEqualTo(rolledUpValue);

        //new roll up mask on statKey2
        Assertions.assertThat(statKey2.getRollupMask()).isEqualTo(newRollUpBitMask);
        Assertions.assertThat(statKey.getRollupMask()).isNotEqualTo(newRollUpBitMask);
    }

    @Test
    public void getBytesAndFromBytes() throws Exception {

        UID statName = UID.from(new byte[] {9,0,0,1});
        UID tag1 = UID.from(new byte[] {9,0,1,0});
        UID tag2 = UID.from(new byte[] {9,0,2,0});
        UID tag1value1 = UID.from(new byte[] {9,0,1,1});
        UID tag2value1 = UID.from(new byte[] {9,0,2,2});
        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40);
        long timeMs = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        long timeMsTruncated = time.toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
        List<TagValue> tagValues = new ArrayList<>();
        tagValues.add(new TagValue(tag1, tag1value1));
        tagValues.add(new TagValue(tag2, tag2value1));

        StatKey statKey = new StatKey(statName, rollUpBitMask, interval, timeMs, tagValues);

        LOGGER.info("statKey: {}", statKey);

        byte[] bytes = statKey.getBytes();

        Assertions.assertThat(bytes).hasSize(4 + RollUpBitMask.BYTE_VALUE_LENGTH +
                EventStoreTimeIntervalEnum.BYTE_VALUE_LENGTH + Long.BYTES + (4 * 4));
        Assertions.assertThat(bytes).containsSubsequence(statName.getUidBytes());
        Assertions.assertThat(bytes).containsSubsequence(rollUpBitMask.asBytes());
        Assertions.assertThat(bytes).containsSubsequence(interval.getByteVal());
        Assertions.assertThat(bytes).containsSubsequence(Bytes.toBytes(timeMsTruncated));
        Assertions.assertThat(bytes).containsSubsequence(tag1.getUidBytes());
        Assertions.assertThat(bytes).containsSubsequence(tag2.getUidBytes());
        Assertions.assertThat(bytes).containsSubsequence(tag1value1.getUidBytes());
        Assertions.assertThat(bytes).containsSubsequence(tag2value1.getUidBytes());

        Assertions.assertThat(statKey.getStatName()).isEqualTo(statName);

        byte[] bytesCopy = Arrays.copyOf(bytes, bytes.length);
        StatKey statKey2 = StatKey.fromBytes(bytesCopy);
        LOGGER.info("statKey2: {}", statKey);

        Assertions.assertThat(statKey2.getBytes()).isEqualTo(bytesCopy);
        Assertions.assertThat(statKey2.getStatName().compareTo(statName)).isEqualTo(0);
        Assertions.assertThat(statKey2.getRollupMask()).isEqualTo(rollUpBitMask);
        Assertions.assertThat(statKey2.getInterval()).isEqualTo(interval);
        Assertions.assertThat(statKey2.getTimeMs()).isEqualTo(timeMsTruncated);
    }


    @Test
    public void compareIntervalPart_isEqual() throws Exception {

        UID statName = UID.from(new byte[] {9,0,0,1});

        UID tag1 = UID.from(new byte[] {9,0,1,0});
        UID tag1value1 = UID.from(new byte[] {9,0,1,1});

        UID tag2 = UID.from(new byte[] {9,0,2,0});
        UID tag2value1 = UID.from(new byte[] {9,0,2,2});
        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40);
        long timeMs = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        List<TagValue> tagValues = new ArrayList<>();
        tagValues.add(new TagValue(tag1, tag1value1));
        tagValues.add(new TagValue(tag2, tag2value1));

        StatKey statKey = new StatKey(statName, rollUpBitMask, interval, timeMs, tagValues);

        EventStoreTimeIntervalEnum interval2 = EventStoreTimeIntervalEnum.MINUTE;

        Assertions.assertThat(statKey.equalsIntervalPart(interval2)).isTrue();
    }

    @Test
    public void compareIntervalPart_isNotEqual() throws Exception {

        UID statName = UID.from(new byte[] {9,0,0,1});

        UID tag1 = UID.from(new byte[] {9,0,1,0});
        UID tag1value1 = UID.from(new byte[] {9,0,1,1});

        UID tag2 = UID.from(new byte[] {9,0,2,0});
        UID tag2value1 = UID.from(new byte[] {9,0,2,2});

        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;
        LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40);
        long timeMs = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        List<TagValue> tagValues = new ArrayList<>();
        tagValues.add(new TagValue(tag1, tag1value1));
        tagValues.add(new TagValue(tag2, tag2value1));

        StatKey statKey = new StatKey(statName, rollUpBitMask, interval, timeMs, tagValues);

        EventStoreTimeIntervalEnum interval2 = EventStoreTimeIntervalEnum.MINUTE;

        Assertions.assertThat(statKey.equalsIntervalPart(interval2)).isFalse();
    }

}