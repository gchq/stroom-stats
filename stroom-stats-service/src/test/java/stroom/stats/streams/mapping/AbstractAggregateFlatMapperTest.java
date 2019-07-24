package stroom.stats.streams.mapping;

import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import stroom.stats.api.StatisticType;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.v4.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.aggregation.StatAggregate;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class AbstractAggregateFlatMapperTest {
    protected MockStatisticConfigurationService mockStatisticConfigurationService;
    protected MockStroomPropertyService mockStroomPropertyService;
    protected UniqueIdCache uniqueIdCache = new MockUniqueIdCache();
    protected String statName = "MyStat";
    protected String statUuid = UUID.randomUUID().toString();
    protected String tag1 = "tag1";
    protected String tag2 = "tag2";
    String tag1val1 = tag1 + "val1";
    String tag2val1 = tag2 + "val1";
    UID statNameUid = uniqueIdCache.getOrCreateId("MyStat");
    UID tag1Uid = uniqueIdCache.getOrCreateId(tag1);
    UID tag2Uid = uniqueIdCache.getOrCreateId(tag2);
    UID tag1val1Uid = uniqueIdCache.getOrCreateId(tag1val1);
    UID tag2val1Uid = uniqueIdCache.getOrCreateId(tag2val1);
    UID rolledUpUid = uniqueIdCache.getOrCreateId(RollUpBitMask.ROLL_UP_TAG_VALUE);
    long id1part1 = 5001L;
    long id1part2 = 1001L;
    long id2part1 = 5002L;
    long id2part2 = 1002L;

    @Before
    public void setupSuper() {
        mockStatisticConfigurationService = new MockStatisticConfigurationService();
        mockStroomPropertyService = new MockStroomPropertyService();
    }


    @Test
    public void testBumpUpInterval_SecondToSecond() {
        doBumpUpTest(buildStatistic(Duration.ZERO), EventStoreTimeIntervalEnum.SECOND);
    }

    @Test
    public void testBumpUpInterval_SecondToMinute() {
        doBumpUpTest(buildStatistic(Duration.ofHours(2)), EventStoreTimeIntervalEnum.MINUTE);
    }

    @Test
    public void testBumpUpInterval_SecondToHour() {
        doBumpUpTest(buildStatistic(Duration.ofDays(2)), EventStoreTimeIntervalEnum.HOUR);
    }

    @Test
    public void testBumpUpInterval_SecondToDay() {
        doBumpUpTest(buildStatistic(Duration.ofDays(7*8)), EventStoreTimeIntervalEnum.DAY);
    }

    @Test
    public void testBumpUpInterval_SecondToForever() {
        doBumpUpTest(buildStatistic(Duration.ofDays(365*2)), EventStoreTimeIntervalEnum.FOREVER);
    }

    abstract AbstractStatisticFlatMapper getFlatMapper();

    abstract StatisticType getStatisticType();

    abstract Statistics.Statistic buildStatistic(final Duration age);




    protected void setPurgeRetention(final EventStoreTimeIntervalEnum interval, final int newValue) {
        mockStroomPropertyService.setProperty(
                HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX +
                        interval.name().toLowerCase(), newValue);
    }

    /**
     * Event time and purge retention on each interval size means each event will
     * get bumped up to the next interval size
     * SEC -> MIN
     * MIN -> HOUR
     * HOUR -> DAY
     * DAY -> FOREVER
     */
    protected void doBumpUpTest(final Statistics.Statistic statistic, final EventStoreTimeIntervalEnum expectedInterval) {
        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            //one row key interval (see EventStoreTimeIntervalEnum for details) of retention
            setPurgeRetention(interval, 1);
        }

        ZonedDateTime timeNow = ZonedDateTime.now(ZoneOffset.UTC);

        EventStoreTimeIntervalEnum precision = EventStoreTimeIntervalEnum.SECOND;

        StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration()
                .setUuid(statUuid)
                .setName(statName)
                .setStatisticType(getStatisticType())
                .setRollUpType(StatisticRollUpType.ALL)
                .addFieldNames(tag1, tag2)
                .setPrecision(precision);

        StatisticWrapper statisticWrapper = new StatisticWrapper(statistic, statisticConfiguration);

        final Iterable<KeyValue<StatEventKey, StatAggregate>> keyValues = getFlatMapper()
                .flatMap(statUuid, statisticWrapper);

        // two tags and rollup all so 4 kvs expected
        Assertions.assertThat(keyValues)
                .hasSize(4);

        Assertions
                .assertThat(StreamSupport.stream(keyValues.spliterator(), false)
                        .map(keyValue -> keyValue.key.getInterval())
                        .collect(Collectors.toList()))
                .containsOnly(expectedInterval);
    }
}
