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

package stroom.stats.streams.mapping;

import javaslang.Tuple2;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.CustomRollUpMask;
import stroom.stats.configuration.MockCustomRollupMask;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.test.StatisticsHelper;

import javax.xml.datatype.DatatypeConfigurationException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCountStatToAggregateFlatMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestCountStatToAggregateFlatMapper.class);

    UniqueIdCache uniqueIdCache = new MockUniqueIdCache();
    MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();

    String statName = "MyStat";
    String tag1 = "tag1";
    String tag2 = "tag2";
    String tag1val1 = tag1 + "val1";
    String tag2val1 = tag2 + "val1";
    UID statNameUid = uniqueIdCache.getOrCreateId("MyStat");
    UID tag1Uid = uniqueIdCache.getOrCreateId(tag1);
    UID tag2Uid = uniqueIdCache.getOrCreateId(tag2);
    UID tag1val1Uid = uniqueIdCache.getOrCreateId(tag1val1);
    UID tag2val1Uid = uniqueIdCache.getOrCreateId(tag2val1);
    UID rolledUpUid = uniqueIdCache.getOrCreateId(RollUpBitMask.ROLL_UP_TAG_VALUE);

    ZonedDateTime time = ZonedDateTime.of(
            LocalDateTime.of(2017, 2, 27, 10, 50, 30),
            ZoneOffset.UTC);
    StatisticType statisticType = StatisticType.COUNT;
    EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
    long aggValue = 1L;

    CountStatToAggregateFlatMapper countStatToAggregateFlatMapper = new CountStatToAggregateFlatMapper(
            uniqueIdCache,
            mockStroomPropertyService);

    @Test
    public void flatMap_noRollUps() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper(StatisticRollUpType.NONE);

        List<KeyValue<StatKey, StatAggregate>> keyValues = new ArrayList<>();
        countStatToAggregateFlatMapper.flatMap(statName, statisticWrapper)
                .forEach(keyValue -> keyValues.add(keyValue));

        assertThat(keyValues).hasSize(1);

        StatKey statKey = keyValues.get(0).key;
        StatAggregate statAggregate = keyValues.get(0).value;

        assertThat(uniqueIdCache.getName(statKey.getStatName())).isEqualTo(statName);
        assertThat(statKey.getRollupMask()).isEqualTo(RollUpBitMask.ZERO_MASK);
        assertThat(statKey.getTimeMs()).isEqualTo(time.toInstant().truncatedTo(ChronoUnit.MINUTES).toEpochMilli());

        assertThat(statKey.getTagValues()).containsExactly(
                new TagValue(tag1Uid, tag1val1Uid),
                new TagValue(tag2Uid, tag2val1Uid));

        assertThat(statAggregate).isInstanceOf(CountAggregate.class);
        CountAggregate countAggregate = (CountAggregate) statAggregate;
        assertThat(countAggregate.getAggregatedCount()).isEqualTo(aggValue);
    }

    @Test
    public void flatMap_allRolledUp() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper(StatisticRollUpType.ALL);

        List<KeyValue<StatKey, StatAggregate>> keyValues = new ArrayList<>();
        countStatToAggregateFlatMapper.flatMap(statName, statisticWrapper)
                .forEach(keyValue -> keyValues.add(keyValue));

        //two tags so 4 possible rollup combos
        assertThat(keyValues).hasSize(4);

        List<Tuple2<TagValue, TagValue>> tagValuePairs = keyValues.stream()
                .map(kv -> new Tuple2<>(kv.key.getTagValues().get(0), kv.key.getTagValues().get(1)))
                .collect(Collectors.toList());

        assertThat(tagValuePairs).containsExactlyInAnyOrder(
                new Tuple2<>(new TagValue(tag1Uid, tag1val1Uid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, tag1val1Uid), new TagValue(tag2Uid, rolledUpUid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, rolledUpUid)));

        assertThat(keyValues.stream()
                .map(kv -> kv.key.getRollupMask())
                .collect(Collectors.toList())
        )
                .containsExactlyInAnyOrder(
                        RollUpBitMask.fromTagPositions(Arrays.asList()),
                        RollUpBitMask.fromTagPositions(Arrays.asList(0)),
                        RollUpBitMask.fromTagPositions(Arrays.asList(1)),
                        RollUpBitMask.fromTagPositions(Arrays.asList(0, 1))
                );

        List<Long> aggValues = keyValues.stream()
                .map(kv -> ((CountAggregate) kv.value).getAggregatedCount())
                .distinct()
                .collect(Collectors.toList());
        //all the same agg value
        assertThat(aggValues).hasSize(1);
        assertThat(aggValues.get(0)).isEqualTo(aggValue);
    }

    @Test
    public void flatMap_customMasks() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper(
                StatisticRollUpType.CUSTOM,
                new MockCustomRollupMask(Arrays.asList(0)),
                new MockCustomRollupMask(Arrays.asList(0, 1)));

        List<KeyValue<StatKey, StatAggregate>> keyValues = new ArrayList<>();
        countStatToAggregateFlatMapper.flatMap(statName, statisticWrapper)
                .forEach(keyValue -> keyValues.add(keyValue));

        //two custom rollup masks so should get 3 different combos, the zero mask plus the 2 custom ones
        assertThat(keyValues).hasSize(3);

        List<Tuple2<TagValue, TagValue>> tagValuePairs = keyValues.stream()
                .map(kv -> new Tuple2<>(kv.key.getTagValues().get(0), kv.key.getTagValues().get(1)))
                .collect(Collectors.toList());

        assertThat(tagValuePairs).containsExactlyInAnyOrder(
                new Tuple2<>(new TagValue(tag1Uid, tag1val1Uid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, rolledUpUid)));

        assertThat(keyValues.stream()
                .map(kv -> kv.key.getRollupMask())
                .collect(Collectors.toList())
        )
                .containsExactlyInAnyOrder(
                        RollUpBitMask.fromTagPositions(Arrays.asList()),
                        RollUpBitMask.fromTagPositions(Arrays.asList(0)),
                        RollUpBitMask.fromTagPositions(Arrays.asList(0, 1))
                );

        List<Long> aggValues = keyValues.stream()
                .map(kv -> ((CountAggregate) kv.value).getAggregatedCount())
                .distinct()
                .collect(Collectors.toList());
        //all the same agg value
        assertThat(aggValues).hasSize(1);
        assertThat(aggValues.get(0)).isEqualTo(aggValue);
    }

    private StatisticWrapper buildBasicStatWrapper(final StatisticRollUpType statisticRollUpType, CustomRollUpMask... customRollUpMasks)
            throws DatatypeConfigurationException {


        Statistics.Statistic statistic = StatisticsHelper.buildCountStatistic(statName, time, aggValue,
                StatisticsHelper.buildTagType(tag1, tag1val1),
                StatisticsHelper.buildTagType(tag2, tag2val1)
        );

        MockStatisticConfiguration statConfig = new MockStatisticConfiguration()
                .setName(statName)
                .setStatisticType(statisticType)
                .setRollUpType(statisticRollUpType)
                .addFieldName(tag1)
                .addFieldName(tag2)
                .setPrecision(interval.columnInterval());

        for (CustomRollUpMask customRollUpMask : customRollUpMasks) {
            statConfig.addCustomRollupMask(customRollUpMask);
        }

        return new StatisticWrapper(statistic, Optional.of(statConfig));
    }

}