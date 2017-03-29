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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.api.StatisticType;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.MockCustomRollupMask;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.schema.TagType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.test.StatisticsHelper;

import javax.xml.datatype.DatatypeConfigurationException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestValueStatToAggregateFlatMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestValueStatToAggregateFlatMapper.class);

    private MockStatisticConfigurationService mockStatisticConfigurationService;
    private MockStroomPropertyService mockStroomPropertyService;
    private ValueStatToAggregateFlatMapper valueStatToAggregateMapper;
    private UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    private String statName = "MyStat";
    private String tag1 = "tag1";
    private String tag2 = "tag2";
    String tag1val1 = tag1 + "val1";
    String tag2val1 = tag2 + "val1";
    UID statNameUid = uniqueIdCache.getOrCreateId("MyStat");
    UID tag1Uid = uniqueIdCache.getOrCreateId(tag1);
    UID tag2Uid = uniqueIdCache.getOrCreateId(tag2);
    UID tag1val1Uid = uniqueIdCache.getOrCreateId(tag1val1);
    UID tag2val1Uid = uniqueIdCache.getOrCreateId(tag2val1);
    UID rolledUpUid = uniqueIdCache.getOrCreateId(RollUpBitMask.ROLL_UP_TAG_VALUE);
    private long id1part1 = 5001L;
    private long id1part2 = 1001L;
    private long id2part1 = 5002L;
    private long id2part2 = 1002L;

    @Before
    public void setup() {
        mockStatisticConfigurationService = new MockStatisticConfigurationService();
        mockStroomPropertyService = new MockStroomPropertyService();
        valueStatToAggregateMapper = new ValueStatToAggregateFlatMapper(uniqueIdCache, mockStroomPropertyService);
    }


    @Test(expected = RuntimeException.class)
    public void flatMap_noStatConfig() throws Exception {


        Statistics.Statistic statistic = buildStatistic();
        StatisticWrapper statisticWrapper = new StatisticWrapper(statistic, Optional.empty());

        //will throw a RTE as there is no StatConfig
        valueStatToAggregateMapper.flatMap(statistic.getName(), statisticWrapper);
    }

    @Test
    public void flatMap_noRollups() throws Exception {

        Statistics.Statistic statistic = buildStatistic();

        StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration()
                .setName(statistic.getName())
                .setStatisticType(StatisticType.VALUE)
                .setRollUpType(StatisticRollUpType.NONE)
                .addFieldNames(tag1, tag2)
                .setPrecision(1_000L);


        mockStatisticConfigurationService.addStatisticConfiguration(statisticConfiguration);

        StatisticWrapper statisticWrapper = new StatisticWrapper(statistic, Optional.of(statisticConfiguration));

        //will throw a RTE as there is no StatConfig
        Iterable<KeyValue<StatKey, StatAggregate>> iterable = valueStatToAggregateMapper.flatMap(statistic.getName(), statisticWrapper);
        List<KeyValue<StatKey, StatAggregate>> keyValues = (List<KeyValue<StatKey, StatAggregate>>) iterable;

        assertThat(keyValues).hasSize(1);
        assertOnKeyValue(keyValues.get(0), statistic, statisticConfiguration);

        StatKey statKey = keyValues.get(0).key;
        //make sure the values match
        assertThat(statKey.getTagValues().stream()
                        .map(tagValue -> uniqueIdCache.getName(tagValue.getValue()))
                        .collect(Collectors.toList()))
                .isEqualTo(statistic.getTags().getTag().stream()
                        .map(TagType::getValue)
                        .collect(Collectors.toList()));
        assertThat(statKey.getInterval()).isEqualTo(EventStoreTimeIntervalEnum.fromColumnInterval(statisticConfiguration.getPrecision()));

        assertThat(statKey.getRollupMask()).isEqualTo(RollUpBitMask.ZERO_MASK);
    }

    @Test
    public void flatMap_AllRollups() throws Exception {

        Statistics.Statistic statistic = buildStatistic();

        StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration()
                .setName(statistic.getName())
                .setStatisticType(StatisticType.VALUE)
                .setRollUpType(StatisticRollUpType.ALL)
                .addFieldNames(tag1, tag2)
                .setPrecision(1_000L);


        mockStatisticConfigurationService.addStatisticConfiguration(statisticConfiguration);

        StatisticWrapper statisticWrapper = new StatisticWrapper(statistic, Optional.of(statisticConfiguration));

        //will throw a RTE as there is no StatConfig
        Instant start = Instant.now();
        Iterable<KeyValue<StatKey, StatAggregate>> iterable = valueStatToAggregateMapper.flatMap(statistic.getName(), statisticWrapper);
        Duration executionTime = Duration.between(start, Instant.now());
        LOGGER.debug("Execution time: {}ms", executionTime.toMillis());

        List<KeyValue<StatKey, StatAggregate>> keyValues = (List<KeyValue<StatKey, StatAggregate>>) iterable;

        keyValues.stream().forEach(kv -> LOGGER.debug(kv.key.toString()));

        //2 tags so 4 rolled up events
        assertThat(keyValues).hasSize(4);

        assertThat(keyValues.stream()
                        .map(keyValue -> keyValue.key.getRollupMask())
                        .collect(Collectors.toList()))
                .containsAll(RollUpBitMask.getRollUpBitMasks(2));

        List<Tuple2<TagValue, TagValue>> tagValuePairs = keyValues.stream()
                .map(kv -> new Tuple2<>(kv.key.getTagValues().get(0), kv.key.getTagValues().get(1)))
                .collect(Collectors.toList());

        assertThat(tagValuePairs).containsExactlyInAnyOrder(
                new Tuple2<>(new TagValue(tag1Uid, tag1val1Uid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, tag1val1Uid), new TagValue(tag2Uid, rolledUpUid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, rolledUpUid)));

        keyValues.forEach(keyValue -> assertOnKeyValue(keyValue, statistic, statisticConfiguration));
    }

    @Test
    public void flatMap_CustomRollups() throws Exception {

        Statistics.Statistic statistic = buildStatistic();

        RollUpBitMask mask0 = RollUpBitMask.ZERO_MASK;
        RollUpBitMask mask1 = RollUpBitMask.fromTagPositions(Collections.singletonList(0));
        RollUpBitMask mask2 = RollUpBitMask.fromTagPositions(Arrays.asList(0, 1));

        StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration()
                .setName(statistic.getName())
                .setStatisticType(StatisticType.VALUE)
                .setRollUpType(StatisticRollUpType.CUSTOM)
                .addCustomRollupMask(new MockCustomRollupMask(mask1.getTagPositionsAsList()))
                .addCustomRollupMask(new MockCustomRollupMask(mask2.getTagPositionsAsList()))
                .addFieldNames(tag1, tag2)
                .setPrecision(1_000L);


        mockStatisticConfigurationService.addStatisticConfiguration(statisticConfiguration);

        StatisticWrapper statisticWrapper = new StatisticWrapper(statistic, Optional.of(statisticConfiguration));

        //will throw a RTE as there is no StatConfig
        Instant start = Instant.now();
        Iterable<KeyValue<StatKey, StatAggregate>> iterable = valueStatToAggregateMapper.flatMap(statistic.getName(), statisticWrapper);
        Duration executionTime = Duration.between(start, Instant.now());
        LOGGER.debug("Execution time: {}ms", executionTime.toMillis());

        List<KeyValue<StatKey, StatAggregate>> keyValues = (List<KeyValue<StatKey, StatAggregate>>) iterable;

        //2 custom masks and the zero mask will always be generated so expect 3 events in total
        assertThat(keyValues).hasSize(3);

        assertThat(keyValues.stream()
                        .map(keyValue -> keyValue.key.getRollupMask())
                        .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        mask0,
                        mask1,
                        mask2);

        List<Tuple2<TagValue, TagValue>> tagValuePairs = keyValues.stream()
                .map(kv -> new Tuple2<>(kv.key.getTagValues().get(0), kv.key.getTagValues().get(1)))
                .collect(Collectors.toList());

        assertThat(tagValuePairs).containsExactlyInAnyOrder(
                new Tuple2<>(new TagValue(tag1Uid, tag1val1Uid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, tag2val1Uid)),
                new Tuple2<>(new TagValue(tag1Uid, rolledUpUid), new TagValue(tag2Uid, rolledUpUid)));

        keyValues.forEach(keyValue -> assertOnKeyValue(keyValue, statistic, statisticConfiguration));
    }

    @Test
    public void flatMap_AllRollUps_noTags() throws Exception {

        Statistics.Statistic statistic = buildStatisticNoTags();

        StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration()
                .setName(statistic.getName())
                .setStatisticType(StatisticType.COUNT)
                .setRollUpType(StatisticRollUpType.ALL)
                .addFieldNames(tag1, tag2)
                .setPrecision(1_000L);


        mockStatisticConfigurationService.addStatisticConfiguration(statisticConfiguration);

        StatisticWrapper statisticWrapper = new StatisticWrapper(statistic, Optional.of(statisticConfiguration));

        Instant start = Instant.now();
        Iterable<KeyValue<StatKey, StatAggregate>> iterable = valueStatToAggregateMapper.flatMap(statistic.getName(), statisticWrapper);
        Duration executionTime = Duration.between(start, Instant.now());
        LOGGER.debug("Execution time: {}ms", executionTime.toMillis());

        List<KeyValue<StatKey, StatAggregate>> keyValues = (List<KeyValue<StatKey, StatAggregate>>) iterable;

        //no tags so nothing to rollup, thus just get the original event
        assertThat(keyValues).hasSize(1);

    }

    private void assertOnKeyValue(KeyValue<StatKey, StatAggregate> keyValue, Statistics.Statistic statistic, StatisticConfiguration statisticConfiguration) {
        StatKey statKey = keyValue.key;
        StatAggregate statAggregate = keyValue.value;
        assertThat(statKey).isNotNull();
        assertThat(statKey.getTagValues()).hasSize(statistic.getTags().getTag().size());
        //make sure the tags match
        assertThat(statKey.getTagValues().stream()
                        .map(tagValue -> uniqueIdCache.getName(tagValue.getTag()))
                        .collect(Collectors.toList()))
                .isEqualTo(statistic.getTags().getTag().stream()
                        .map(TagType::getName)
                        .collect(Collectors.toList()));
        assertThat(statKey.getInterval()).isEqualTo(EventStoreTimeIntervalEnum.fromColumnInterval(statisticConfiguration.getPrecision()));

        assertThat(statAggregate).isNotNull();
        assertThat(statAggregate).isExactlyInstanceOf(ValueAggregate.class);

        ValueAggregate valueAggregate = (ValueAggregate) statAggregate;
        assertThat(valueAggregate.getAggregatedValue()).isEqualTo(statistic.getValue());
        assertThat(valueAggregate.getEventIds()).hasSize(2);

        MultiPartIdentifier id1 = valueAggregate.getEventIds().get(0);
        assertThat(id1.getValue()).contains(id1part1, id1part2);
        MultiPartIdentifier id2 = valueAggregate.getEventIds().get(1);
        assertThat(id2.getValue()).contains(id2part1, id2part2);
    }


    private Statistics.Statistic buildStatistic() throws DatatypeConfigurationException {

        //use system time as class under test has logic based on system time
        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics.Statistic statistic = StatisticsHelper.buildValueStatistic(statName, time, 10.5,
                StatisticsHelper.buildTagType(tag1, tag1val1),
                StatisticsHelper.buildTagType(tag2, tag2val1)
        );
        StatisticsHelper.addIdentifier(statistic, id1part1, id1part2);
        StatisticsHelper.addIdentifier(statistic, id2part1, id2part2);

        return statistic;
    }


    private Statistics.Statistic buildStatisticNoTags() throws DatatypeConfigurationException {

        //use system time as class under test has logic based on system time
        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics.Statistic statistic = StatisticsHelper.buildValueStatistic(statName, time, 10L);

        StatisticsHelper.addIdentifier(statistic, id1part1, id1part2);
        StatisticsHelper.addIdentifier(statistic, id2part1, id2part2);

        return statistic;
    }

}