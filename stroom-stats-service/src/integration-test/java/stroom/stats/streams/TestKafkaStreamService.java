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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import javaslang.Tuple3;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.hibernate.SessionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import stroom.stats.StroomStatsEmbeddedOverrideModule;
import stroom.stats.StroomStatsServiceModule;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.config.Config;
import stroom.stats.config.ZookeeperConfig;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.stats.test.StatisticsHelper;
import stroom.stats.xml.StatisticsMarshaller;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaStreamService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaStreamService.class);

    public static final String STATISTIC_EVENTS_TOPIC_PREFIX = "statisticEvents";
    public static final String BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "badStatisticEvents";
    public static final String STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "statisticRollupPerms";

    private static final Map<StatisticType, String> INPUT_TOPICS_MAP = new HashMap<>();
    private static final Map<StatisticType, String> BAD_TOPICS_MAP = new HashMap<>();
    private static final Map<StatisticType, List<String>> ROLLUP_TOPICS_MAP = new HashMap<>();

    private final StatisticsMarshaller statisticsMarshaller;

    //start kafka/ZK before each test and shut it down after the test has finished
    @Rule
    public KafkaEmbedded kafkaEmbedded = buildEmbeddedKafka();

    MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();

    private UniqueIdCache uniqueIdCache;

    private List<Tuple3<StatisticType, EventStoreTimeIntervalEnum, Map<StatKey, StatAggregate>>> statServiceArguments = new ArrayList<>();

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            //prefix the streams app IDs with the test method name so there are no clashes between tests as the embeded kafka
            //doesn't seem to clean up properly after itself
            setAppIdPrefixes(description.getMethodName());
        }
    };

    @Before
    public void setup() {
        statServiceArguments.clear();
    }

    public TestKafkaStreamService() throws JAXBException {
        statisticsMarshaller = new StatisticsMarshaller();
    }

    private void setAppIdPrefixes(final String extraPrefix) {
        String existingPrefix = mockStroomPropertyService.getPropertyOrThrow(KafkaStreamService.PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX);

        String newPrefix = extraPrefix + existingPrefix;
        mockStroomPropertyService.setProperty(KafkaStreamService.PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX, newPrefix);

        //TODO will also need to change the prefix for the aggregator processor
    }


    @Test
    public void test_TwoGoodCountEventsRollUpAll() throws ExecutionException, InterruptedException, DatatypeConfigurationException {

        setAppIdPrefixes("");
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);
        String statName = "MyStat";

        String tag1 = "tag1";
        String tag2 = "tag2";

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;

        addStatConfig(module.getMockStatisticConfigurationService(),
                statName,
                statisticType,
                Arrays.asList(tag1, tag2),
                interval.columnInterval());

        ZonedDateTime time = ZonedDateTime.of(
                LocalDateTime.of(2017, 2, 27, 10, 50, 30),
                ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(statName, time, 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                StatisticsHelper.buildCountStatistic(statName, time.plusDays(2), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                )
        );

        //Set a long purge retention to stop events being bumped up into the next interval
        setPurgeRetention(interval, Integer.MAX_VALUE);

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //2 input msgs, each one is rolled up to 4 perms and each one iterates up through 4 interval buckets so expect 32 in all
        int expectedGoodPerIntervalTopic = 2 * 4;
        int expectedGoodMsgCount = expectedGoodPerIntervalTopic * 4;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 1000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();

        //both events go to same interval topic
        assertThat(topicToMsgsMap).hasSize(4);
        topicToMsgsMap.entrySet().forEach(entry -> {
            List<ConsumerRecord<StatKey, StatAggregate>> messages = entry.getValue();
            assertThat(messages).hasSize(expectedGoodPerIntervalTopic);
            String topicName = entry.getKey();
            assertThat(messages.stream()
                    .map(rec -> TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, StatisticType.COUNT, rec.key().getInterval()))
                    .distinct()
                    .collect(Collectors.toList())
            )
                    .containsExactly(topicName);
        });

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);

        //now verify the right data vas passed to the stat service

        List<Map.Entry<StatKey, StatAggregate>> passedStats = statServiceArguments.stream()
                .flatMap(invocation -> invocation._3().entrySet().stream())
                .sorted(Comparator.comparing(entry -> entry.getKey()))
                .collect(Collectors.toList());

        if (passedStats.size() != expectedGoodMsgCount) {
            Assert.fail();
        }

        //Make sure all events get passed to the StatService
        assertThat(passedStats.size()).isEqualTo(expectedGoodMsgCount);
    }

    @Test
    public void test_TwoGoodValueEventsRollUpAll() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.VALUE;
        String topic = INPUT_TOPICS_MAP.get(statisticType);
        String statName = "MyStat";

        String tag1 = "tag1";
        String tag2 = "tag2";

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;

        addStatConfig(module.getMockStatisticConfigurationService(),
                statName,
                statisticType,
                Arrays.asList(tag1, tag2),
                interval.columnInterval());

        ZonedDateTime time = ZonedDateTime.of(
                LocalDateTime.of(2017, 2, 27, 10, 50, 30),
                ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildValueStatistic(statName, time, 1.5,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                StatisticsHelper.buildValueStatistic(statName, time.plusHours(2), 1.5,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                )
        );
        dumpStatistics(statistics);

        //Set a long purge retention to stop events being bumped up into the next interval
        setPurgeRetention(interval, Integer.MAX_VALUE);

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //2 input msgs, each one is rolled up to 4 perms so expect 8
        int expectedGoodMsgCount = 8;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(statisticType, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 1000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();

        //both events go to same interval topic
        assertThat(topicToMsgsMap).hasSize(1);
        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
        assertThat(messages.size()).isEqualTo(expectedGoodMsgCount);

        //no bad events
        assertThat(badEvents.size()).isEqualTo(expectedBadMsgCount);
    }


    @Test
    public void test_OneGoodEventPerIntervalAndStatType() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            setPurgeRetention(interval, 10_000);
        }

        ZonedDateTime time = ZonedDateTime.of(
                LocalDateTime.of(2017, 2, 27, 10, 50, 30),
                ZoneOffset.UTC);

        for (StatisticType statisticType : StatisticType.values()) {
            String inputTopic = INPUT_TOPICS_MAP.get(statisticType);

            for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
                String statName = TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval);

                String tag1 = TopicNameFactory.getIntervalTopicName("tag1", statisticType, interval);
                String tag2 = TopicNameFactory.getIntervalTopicName("tag2", statisticType, interval);

                addStatConfig(module.getMockStatisticConfigurationService(),
                        statName,
                        statisticType,
                        Arrays.asList(tag1, tag2),
                        interval.columnInterval());

                Statistics statistics;
                if (statisticType.equals(StatisticType.COUNT)) {
                    statistics = StatisticsHelper.buildStatistics(
                            StatisticsHelper.buildCountStatistic(statName, time, 1L,
                                    StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                                    StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                            )
                    );
                } else {
                    statistics = StatisticsHelper.buildStatistics(
                            StatisticsHelper.buildValueStatistic(statName, time, 1.5,
                                    StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                                    StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                            )
                    );
                }
                dumpStatistics(statistics);
                LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), inputTopic);
                producer.send(buildProducerRecord(inputTopic, statistics)).get();
            }
        }

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //8 input msgs, each one is rolled up to 4 perms so expect 32 in total
        int expectedTopicsPerStatType = 4;
        int expectedTopicCount = 2 * 4;
        int expectedPermsPerMsg = 4;
        int expectedGoodMsgCount = expectedTopicCount * expectedPermsPerMsg;
        int expectedGoodMsgCountPerStatType = expectedTopicsPerStatType * expectedPermsPerMsg;
        int expectedBadMsgCount = 0;

        CountDownLatch countIntervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCountPerStatType, topicToMsgsMap, true, 1000);
        CountDownLatch valueIntervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.VALUE, consumerProps, expectedGoodMsgCountPerStatType, topicToMsgsMap, true, 1000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        countIntervalTopicsLatch.await(10, TimeUnit.SECONDS);
        assertThat(countIntervalTopicsLatch.getCount()).isEqualTo(0);
        valueIntervalTopicsLatch.await(10, TimeUnit.SECONDS);
        assertThat(valueIntervalTopicsLatch.getCount()).isEqualTo(0);

        assertThat(badTopicsLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(topicToMsgsMap).hasSize(expectedTopicCount);
        topicToMsgsMap.entrySet().forEach(entry -> assertThat(entry.getValue()).hasSize(expectedPermsPerMsg));
        topicToMsgsMap.entrySet().forEach(entry -> {
            UID statNameUid = entry.getValue().stream().findFirst().get().key().getStatName();
            String statName = uniqueIdCache.getName(statNameUid);
            String topicName = entry.getKey();
            assertThat(statName).isEqualTo(topicName);
        });

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

    @Test
    public void test_oneGoodOneBad() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);
        String statName = "MyStat";
        String badStatName = "badStatName";

        String tag1 = "tag1";
        String tag2 = "tag2";

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;

        addStatConfig(module.getMockStatisticConfigurationService(),
                statName,
                statisticType,
                Arrays.asList(tag1, tag2),
                interval.columnInterval());

        ZonedDateTime time = ZonedDateTime.of(
                LocalDateTime.of(2017, 2, 27, 10, 50, 30),
                ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                //the good
                StatisticsHelper.buildCountStatistic(statName, time, 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                //the bad
                StatisticsHelper.buildCountStatistic(badStatName, time.plusHours(2), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                )
        );

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //1 good input msgs, each one is rolled up to 4 perms so expect 4
        int expectedGoodMsgCount = 4;
        int expectedBadMsgCount = 1;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT,
                consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 1000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();

        //only one interval topic
        assertThat(topicToMsgsMap).hasSize(1);
        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
        assertThat(messages).hasSize(expectedGoodMsgCount);

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
        assertThat(badEvents.values().stream().findFirst().get()).hasSize(expectedBadMsgCount);
        assertThat(badEvents.values().stream().findFirst().get().stream().findFirst().get()).contains(badStatName);
    }

    @Test
    public void test_oneEventOutsideBiggestRetentionOneInside() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            //one row interval for DAY is 52wks
            setPurgeRetention(interval, 1);
        }

        ZonedDateTime timeNow = ZonedDateTime
                .now(ZoneOffset.UTC);

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);
        String statName = "MyStat";

        String tag1 = "tag1";
        String tag2 = "tag2";

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.DAY;

        addStatConfig(module.getMockStatisticConfigurationService(),
                statName,
                statisticType,
                Arrays.asList(tag1, tag2),
                interval.columnInterval());

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(statName, timeNow, 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                //Event time is 2 years ago so will be outside all purge retention thresholds
                StatisticsHelper.buildCountStatistic(statName, timeNow.minusYears(2), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                )
        );

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //1 good input msg, each one is rolled up to 4 perms so expect 4
        int expectedGoodMsgCount = 4;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 1000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();

        //Only the good event goes on the topic, the one outside the retention is just ignored
        assertThat(topicToMsgsMap).hasSize(1);
        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
        assertThat(messages).hasSize(expectedGoodMsgCount);

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

    /**
     * Event time and purge retention on each interval size means each event will
     * get bumped up to the next interval size
     * SEC -> MIN
     * SEC -> HOUR
     * SEC -> DAY
     * SEC -> ignored
     */
    @Test
    public void test_allEventsBumpedToNextInterval() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            //one row key interval (see EventStoreTimeIntervalEnum for details) of retention
            setPurgeRetention(interval, 1);
        }

        ZonedDateTime timeNow = ZonedDateTime
                .now(ZoneOffset.UTC);

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);
        String statName = "MyStat";

        String tag1 = "tag1";
        String tag2 = "tag2";

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;

        addStatConfig(module.getMockStatisticConfigurationService(),
                statName,
                statisticType,
                Arrays.asList(tag1, tag2),
                interval.columnInterval());

        Statistics statistics = StatisticsHelper.buildStatistics(
                //bump up to MIN interval
                StatisticsHelper.buildCountStatistic(statName, timeNow.minusHours(2), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                //bumped up to HOUR interval
                StatisticsHelper.buildCountStatistic(statName, timeNow.minusDays(2), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                //bumped up to DAY interval
                StatisticsHelper.buildCountStatistic(statName, timeNow.minusWeeks(8), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                ),
                //ignored
                StatisticsHelper.buildCountStatistic(statName, timeNow.minusYears(2), 1L,
                        StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                        StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                )
        );

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //3 good input msg, each one is rolled up to 4 perms so expect 12, one input msg ignored
        int expectedGoodMsgCount = 3 * 4;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 1000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(3, TimeUnit.SECONDS)).isTrue();

        //Only the good event goes on the topic, the one outside the retention is just ignored
        assertThat(topicToMsgsMap).hasSize(3);
        Set<String> topicNames = topicToMsgsMap.keySet();
        assertThat(topicNames).contains(
                TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, EventStoreTimeIntervalEnum.MINUTE),
                TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, EventStoreTimeIntervalEnum.HOUR),
                TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, EventStoreTimeIntervalEnum.DAY)
        );
        topicToMsgsMap.values().forEach(val ->
                assertThat(val).hasSize(4)
        );

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

//    @Ignore
    @Test
    public void test_ManyEventsOnMultipleThreads() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        setNumStreamThreads(1);
        StroomStatsEmbeddedOverrideModule module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            setPurgeRetention(interval, 10_000);
        }

        int statNameCnt = 10;
        int msgCntPerStatNameAndIntervalAndType = 100;

        List<ProducerRecord<String, String>> producerRecords = new ArrayList<>();

        long counter = Instant.now().toEpochMilli();

//        StatisticType[] types = new StatisticType[] {StatisticType.COUNT};
        StatisticType[] types = StatisticType.values();
//        EventStoreTimeIntervalEnum[] intervals = new EventStoreTimeIntervalEnum[]{EventStoreTimeIntervalEnum.MINUTE};
        EventStoreTimeIntervalEnum[] intervals = EventStoreTimeIntervalEnum.values();

        for (StatisticType statisticType : types) {
            String inputTopic = INPUT_TOPICS_MAP.get(statisticType);

            for (EventStoreTimeIntervalEnum interval : intervals) {
//                EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;

                int cnt = 0;

                for (int statNum : IntStream.rangeClosed(1, statNameCnt).toArray()) {
                    String statName = TopicNameFactory.getIntervalTopicName("MyStat-" + statNum, statisticType, interval);

                    String tag1 = "tag1-" + statName;
                    String tag2 = "tag2-" + statName;

                    addStatConfig(module.getMockStatisticConfigurationService(),
                            statName,
                            statisticType,
                            Arrays.asList(tag1, tag2),
                            interval.columnInterval());

                    for (int i : IntStream.rangeClosed(1, msgCntPerStatNameAndIntervalAndType).toArray()) {

                        Random random = new Random();
                        //Give each source event a different time to aid debugging
                        ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(counter), ZoneOffset.UTC);
                        Statistics statistics;
                        if (statisticType.equals(StatisticType.COUNT)) {
                            statistics = StatisticsHelper.buildStatistics(
                                    StatisticsHelper.buildCountStatistic(statName, time, counter,
                                            StatisticsHelper.buildTagType(tag1, tag1 + "val" + random.nextInt(3)),
                                            StatisticsHelper.buildTagType(tag2, tag2 + "val" + random.nextInt(3))
                                    )
                            );
                        } else {
                            statistics = StatisticsHelper.buildStatistics(
                                    StatisticsHelper.buildValueStatistic(statName, time, counter + 0.5,
                                            StatisticsHelper.buildTagType(tag1, tag1 + "val" + random.nextInt(3)),
                                            StatisticsHelper.buildTagType(tag2, tag2 + "val" + random.nextInt(3))
                                    )
                            );
                        }
                        dumpStatistics(statistics);
                        producerRecords.add(buildProducerRecord(inputTopic, statistics));
                        counter++;
                        cnt++;
                    }
                    LOGGER.info("Added {} records for type {} and interval {}", cnt, statisticType, interval);
                }
            }
        }

        //shuffle all the msgs so the streams consumer gets them in a random order
        Collections.shuffle(producerRecords, new Random());
        LOGGER.info("Sending {} stat events", producerRecords.size());

        //use multiple threads to send the messages asynchronously
        producerRecords
//                .parallelStream()
                .stream()
                .forEach(rec -> {
                    try {
                        producer.send(rec);
                    } catch (Exception e) {
                        throw new RuntimeException("exception sending mesg", e);
                    }
                });

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");

        ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        int expectedTopicsPerStatType = intervals.length;
        int expectedTopicCount = types.length * expectedTopicsPerStatType;
        int expectedPermsPerMsg = 400;
        int expectedGoodMsgCountPerStatTypeAndInterval = statNameCnt *
                expectedPermsPerMsg * msgCntPerStatNameAndIntervalAndType;
        int expectedGoodMsgCountPerStatType = intervals.length * expectedGoodMsgCountPerStatTypeAndInterval;
        int expectedBadMsgCount = 0;

        LOGGER.info("Expecting {} msgs per stat type and interval, {} per stat type",
                expectedGoodMsgCountPerStatTypeAndInterval, expectedGoodMsgCountPerStatType);

        CountDownLatch countIntervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCountPerStatType, topicToMsgsMap, false, 10_000);
        CountDownLatch valueIntervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.VALUE, consumerProps, expectedGoodMsgCountPerStatType, topicToMsgsMap, false, 10_000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);


        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                StringBuilder sb = new StringBuilder("Current state of the topicToMsgsMap:\n");
                topicToMsgsMap.keySet().stream()
                        .sorted()
                        .forEach(key -> sb.append(key + " - " + topicToMsgsMap.get(key).size() + "\n"));
                LOGGER.debug(sb.toString());
            } catch (Exception e) {
                LOGGER.error("Got error in executor: {}", e.getMessage(), e);
            }
        }, 0, 3, TimeUnit.SECONDS);

        //Wait for the expected numbers from messages to arrive or timeout if not
        countIntervalTopicsLatch.await(600, TimeUnit.SECONDS);
        valueIntervalTopicsLatch.await(600, TimeUnit.SECONDS);
        assertThat(badTopicsLatch.await(60, TimeUnit.SECONDS)).isTrue();

        assertThat(countIntervalTopicsLatch.getCount()).isEqualTo(0);

        valueIntervalTopicsLatch.await(60, TimeUnit.SECONDS);
        assertThat(valueIntervalTopicsLatch.getCount()).isEqualTo(0);

        assertThat(badTopicsLatch.await(60, TimeUnit.SECONDS)).isTrue();

        assertThat(topicToMsgsMap).hasSize(expectedTopicCount);
        topicToMsgsMap.entrySet().forEach(entry ->
                assertThat(entry.getValue().size()).isEqualTo(expectedGoodMsgCountPerStatTypeAndInterval));

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

    /**
     * Start a consumer consuming from all time interval topics (one for each interval of the passed stat type so 4 in all)
     * and both log each message and add the message to a map keyed by topic name.
     * A {@link CountDownLatch} is returned to allow the caller to wait for the expected number of messages
     */
    private CountDownLatch startIntervalTopicsConsumer(final StatisticType statisticType,
                                                       final Map<String, Object> consumerProps,
                                                       final int expectedMsgCount,
                                                       final ConcurrentMap<String, List<ConsumerRecord<StatKey, StatAggregate>>> topicToMsgsMap,
                                                       final boolean isEachMsgLogged,
                                                       final int pollIntervalMs) {

        Map<String, Object> consumerProps2 = KafkaTestUtils.consumerProps("consumer-" + statisticType, "true", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "latest");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 2_000);

        final CountDownLatch latch = new CountDownLatch(expectedMsgCount);
        final Serde<StatAggregate> stagAggSerde = StatAggregateSerde.instance();
        final Serde<StatKey> statKeySerde = StatKeySerde.instance();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<StatKey, StatAggregate> kafkaConsumer = new KafkaConsumer<>(consumerProps2,
                    statKeySerde.deserializer(),
                    stagAggSerde.deserializer());

            //Subscribe to all perm topics for this stat type
//            List<String> topics = ROLLUP_TOPICS_MAP.entrySet().stream()
//                    .flatMap(entry -> entry.getValue().stream())
//                    .collect(Collectors.toList());

            List<String> topics = ROLLUP_TOPICS_MAP.get(statisticType);
            LOGGER.info("Starting consumer for type {}, expectedMsgCount {}, topics {}",
                    statisticType, expectedMsgCount, topics);
            kafkaConsumer.subscribe(topics);
            AtomicLong recCounter = new AtomicLong(0);

            try {
                while (true) {
                    try {
                        ConsumerRecords<StatKey, StatAggregate> records = kafkaConsumer.poll(pollIntervalMs);
                        recCounter.addAndGet(records.count());
                        LOGGER.trace("{} consumed {} good records, cumulative count {}", statisticType, records.count(), recCounter.get());
                        for (ConsumerRecord<StatKey, StatAggregate> record : records) {
                            if (isEachMsgLogged) {
                                LOGGER.trace("IntervalTopicsConsumer - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                                StatKeyUtils.logStatKey(uniqueIdCache, record.key());
                            }
                            latch.countDown();
                            topicToMsgsMap.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error while polling with stat type {}", statisticType, e);
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });
        return latch;
    }

    /**
     * Start a consume consuming from both bad events topics, log each message and add each message
     * into a map keyed by topic name
     * A {@link CountDownLatch} is returned to allow the caller to wait for the expected number of messages
     */
    private CountDownLatch startBadEventsConsumer(final Map<String, Object> consumerProps, final int expectedMsgCount,
                                                  Map<String, List<String>> messages) {

        final CountDownLatch latch = new CountDownLatch(expectedMsgCount);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());

            //Subscribe to all bad event topics
            kafkaConsumer.subscribe(BAD_TOPICS_MAP.values());

            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.warn("Bad events Consumer - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        latch.countDown();
                        messages.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record.value());
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });
        return latch;
    }

    private void addStatConfig(MockStatisticConfigurationService mockStatisticConfigurationService,
                               String statName,
                               StatisticType statisticType,
                               List<String> fieldNames,
                               long precision) {
        MockStatisticConfiguration statConfig = new MockStatisticConfiguration()
                .setName(statName)
                .setStatisticType(statisticType)
                .setRollUpType(StatisticRollUpType.ALL)
                .addFieldNames(fieldNames)
                .setPrecision(precision);

        LOGGER.debug("Adding StatConfig: {} {} {} {} {}",
                statConfig.getName(),
                statConfig.getStatisticType(),
                statConfig.getRollUpType(),
                statConfig.getFieldNames(),
                statConfig.getPrecision());

        mockStatisticConfigurationService.addStatisticConfiguration(statConfig);
    }


    private ProducerRecord<String, String> buildProducerRecord(String topic, Statistics statistics) {
        String statName = statistics.getStatistic().get(0).getName();
        return new ProducerRecord<>(topic, statName, statisticsMarshaller.marshallXml(statistics));
    }

    private static KafkaEmbedded buildEmbeddedKafka() {
        List<String> topics = new ArrayList<>();

        //Build a list of all the topics to create and along thw way create a map for each of
        //the topic types
        Arrays.stream(StatisticType.values())
                .forEachOrdered(type -> {
                    String inputTopic = TopicNameFactory.getStatisticTypedName(STATISTIC_EVENTS_TOPIC_PREFIX, type);
                    topics.add(inputTopic);
                    INPUT_TOPICS_MAP.put(type, inputTopic);

                    String badTopic = TopicNameFactory.getStatisticTypedName(BAD_STATISTIC_EVENTS_TOPIC_PREFIX, type);
                    topics.add(badTopic);
                    BAD_TOPICS_MAP.put(type, badTopic);

                    Arrays.stream(EventStoreTimeIntervalEnum.values())
                            .forEachOrdered(interval -> {
                                String rollupTopic = TopicNameFactory.getIntervalTopicName(
                                        STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX,
                                        type,
                                        interval);
                                topics.add(rollupTopic);
                                ROLLUP_TOPICS_MAP.computeIfAbsent(type, k -> new ArrayList<>()).add(rollupTopic);
                            });
                });


        topics.add("testTopic");

        topics.forEach(topic -> LOGGER.info("Creating topic: {}", topic));

        return new KafkaEmbedded(1, true, 1, topics.toArray(new String[topics.size()]));
    }

    private StroomStatsEmbeddedOverrideModule initStreamProcessing() {
        //Set up the properties service so it points to the embedded kafka
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        String zookeeprConnectStr = kafkaEmbedded.getZookeeperConnectionString();

        String bootStrapServersConfig = (String) senderProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        //Override the kafka bootstrap servers prop to point to the embedded kafka rather than the docker one
        mockStroomPropertyService.setProperty(KafkaStreamService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS, bootStrapServersConfig);
        mockStroomPropertyService.setProperty(KafkaStreamService.PROP_KEY_KAFKA_COMMIT_INTERVAL_MS, 1_000);

        SessionFactory mockSessionFactory = Mockito.mock(SessionFactory.class);
        StatisticsService mockStatisticsService = Mockito.mock(StatisticsService.class);

        Mockito
                .doAnswer(invocation -> {
                    statServiceArguments.add(new Tuple3<>(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2)));
                    return null;
                })
                .when(mockStatisticsService)
                .putAggregatedEvents(Mockito.any(), Mockito.any(), Mockito.anyMap());

        StroomStatsEmbeddedOverrideModule embeddedOverrideModule = new StroomStatsEmbeddedOverrideModule(
                mockStroomPropertyService,
                Optional.of(mockStatisticsService));


        ZookeeperConfig mockZookeeperConfig = Mockito.mock(ZookeeperConfig.class);
        Config mockConfig = Mockito.mock(Config.class);
        Mockito.when(mockZookeeperConfig.getQuorum()).thenReturn(kafkaEmbedded.getZookeeperConnectionString());
        Mockito.when(mockConfig.getZookeeperConfig()).thenReturn(mockZookeeperConfig);


        //override the service guice module with our test one that uses mocks for props, stat config and stats service
        Module embeddedServiceModule = Modules
                .override(new StroomStatsServiceModule(mockConfig, mockSessionFactory))
                .with(embeddedOverrideModule);

        Injector injector = Guice.createInjector(embeddedServiceModule);

        //get an instance of the kafkaStreamService so we know it has started up
        injector.getInstance(KafkaStreamService.class);

        uniqueIdCache = injector.getInstance(UniqueIdCache.class);

        return embeddedOverrideModule;
    }

    private void dumpStatistics(Statistics statisticsObj) {
        if (LOGGER.isTraceEnabled()) {
            statisticsObj.getStatistic().forEach(statistic -> {
                String tagValues = statistic.getTags().getTag().stream()
                        .map(tagValue -> tagValue.getName() + "|" + tagValue.getValue())
                        .collect(Collectors.joining(","));
                LOGGER.trace("Stat: {} {} {} {} {}",
                        statistic.getName(),
                        statistic.getTime(),
                        tagValues,
                        statistic.getValue(),
                        statistic.getIdentifiers());
            });
        }
    }

    private void setPurgeRetention(final EventStoreTimeIntervalEnum interval, final int newValue) {
        mockStroomPropertyService.setProperty(
                HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX +
                        interval.name().toLowerCase(), newValue);
    }

    private void setNumStreamThreads(final int newValue) {
        mockStroomPropertyService.setProperty(
                KafkaStreamService.PROP_KEY_KAFKA_STREAM_THREADS, newValue);
    }


}