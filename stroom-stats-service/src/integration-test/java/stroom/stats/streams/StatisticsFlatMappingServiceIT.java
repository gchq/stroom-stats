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
import org.apache.hadoop.util.ThreadUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.SoftAssertions;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
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
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.v4.Statistics;
import stroom.stats.service.config.Config;
import stroom.stats.service.config.ZookeeperConfig;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatEventKeySerde;
import stroom.stats.test.KafkaEmbededUtils;
import stroom.stats.test.StatisticsHelper;
import stroom.stats.schema.v4.StatisticsMarshaller;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static stroom.stats.streams.StatisticsFlatMappingProcessor.PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX;

public class StatisticsFlatMappingServiceIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsFlatMappingServiceIT.class);

    public static final String STATISTIC_EVENTS_TOPIC_PREFIX = "statisticEvents";
    public static final String BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "badStatisticEvents";
    public static final String STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "statisticRollupPerms";

    private static final Map<StatisticType, String> INPUT_TOPICS_MAP = new HashMap<>();
    private static final Map<StatisticType, String> BAD_TOPICS_MAP = new HashMap<>();
    private static final Map<StatisticType, List<String>> ROLLUP_TOPICS_MAP = new HashMap<>();

    private static final String GOOD_STAT_NAME = "MyStat";
    private static final String GOOD_STAT_UUID = StatisticsHelper.getUuidKey(GOOD_STAT_NAME);

    private static final String TAG_1 = "tag1";
    private static final String TAG_2 = "tag2";

    private final StatisticsMarshaller statisticsMarshaller;

    private static final List<String> topics = new ArrayList<>();

    //start kafka/ZK before each test and shut it down after the test has finished
    @ClassRule
    public static KafkaEmbedded kafkaEmbedded = buildEmbeddedKafka();

    private MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();

    private UniqueIdCache uniqueIdCache;
    private Injector injector;
    private StroomStatsEmbeddedOverrideModule module;

    private final AtomicBoolean areConsumersEnabled = new AtomicBoolean(false);
    private final AtomicInteger activeConsumerThreads = new AtomicInteger(0);

//    private List<Tuple3<StatisticType, EventStoreTimeIntervalEnum, Map<StatEventKey, StatAggregate>>> statServiceArguments = new ArrayList<>();

    @Rule
    public TestRule watcher = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            //prefix the streams app IDs with the test method name so there are no clashes between tests as the embeded kafka
            //doesn't seem to clean up properly after itself
            setAppIdPrefixes(description.getMethodName() + Instant.now().toEpochMilli());
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("TRACE enabled");
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("DEBUG enabled");
            }
        }
    };

    @Before
    public void setup() {
//        ThreadUtil.sleepAtLeastIgnoreInterrupts(3_000);
        KafkaEmbededUtils.deleteAndCreateTopics(kafkaEmbedded, topics.toArray(new String[topics.size()]));
        areConsumersEnabled.set(true);
    }

    @After
    public void teardown() {
        Optional.ofNullable(injector)
                .map(ijtr -> ijtr.getInstance(StatisticsFlatMappingService.class))
                .ifPresent(StatisticsFlatMappingService::stop);
//        ThreadUtil.sleepAtLeastIgnoreInterrupts(2_000);
        areConsumersEnabled.set(false);
        while (activeConsumerThreads.get() != 0) {
            //wait for all consumers to stop
        }
        activeConsumerThreads.set(0);
    }

    public StatisticsFlatMappingServiceIT() throws JAXBException {
        statisticsMarshaller = new StatisticsMarshaller();
    }

    private void setAppIdPrefixes(final String extraPrefix) {
        String existingPrefix = mockStroomPropertyService.getPropertyOrThrow(PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX);

        String newPrefix = extraPrefix + existingPrefix;
        mockStroomPropertyService.setProperty(PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX, newPrefix);

        //TODO will also need to change the prefix for the aggregator processor
    }


    @Test
    public void test_TwoGoodCountEventsRollUpAll() throws ExecutionException, InterruptedException, DatatypeConfigurationException {

        setAppIdPrefixes("");
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;

        addStatConfig(module.getMockStatisticConfigurationService(),
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                interval);

        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, time, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, time.plusDays(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );

        //Set a long purge retention to stop events being bumped up into the next interval
        setPurgeRetention(interval, Integer.MAX_VALUE);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "earliest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //2 input msgs, each one is rolled up to 4 perms so expect 8
        int expectedGoodMsgCount = 2 * 4;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 100);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //give the consumers and streams enough time to spin up
//        ThreadUtil.sleepAtLeastIgnoreInterrupts(1_000);

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();
        producer.close();

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();

        //both events go to same interval topic
        assertThat(topicToMsgsMap).hasSize(1);

        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatEventKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
        assertThat(messages).hasSize(expectedGoodMsgCount);

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

    @Test
    public void test_TwoGoodValueEventsRollUpAll() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.VALUE;
        String topic = INPUT_TOPICS_MAP.get(statisticType);

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;

        addStatConfig(module.getMockStatisticConfigurationService(),
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                interval);

        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildValueStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, time, 1.5,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                StatisticsHelper.buildValueStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, time.plusHours(2), 1.5,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );
        dumpStatistics(statistics);

        //Set a long purge retention to stop events being bumped up into the next interval
        setPurgeRetention(interval, Integer.MAX_VALUE);


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "earliest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //2 input msgs, each one is rolled up to 4 perms so expect 8
        int expectedGoodMsgCount = 8;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(statisticType, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 100);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //give the consumers and streams enough time to spin up
//        ThreadUtil.sleepAtLeastIgnoreInterrupts(1_000);

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();
        producer.close();

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();

        //both events go to same interval topic
        assertThat(topicToMsgsMap).hasSize(1);
        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatEventKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
        messages.stream()
                .map(ConsumerRecord::toString)
                .forEach(LOGGER::debug);
        assertThat(messages).hasSize(expectedGoodMsgCount);

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }


    @Test
    public void test_OneGoodEventPerIntervalAndStatType() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            setPurgeRetention(interval, 10_000);
        }

        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //8 input msgs, each one is rolled up to 4 perms so expect 32 in total
        int expectedTopicsPerStatType = 4;
        int expectedTopicCount = 2 * 4;
        int expectedPermsPerMsg = 4;
        int expectedGoodMsgCount = expectedTopicCount * expectedPermsPerMsg;
        int expectedGoodMsgCountPerStatType = expectedTopicsPerStatType * expectedPermsPerMsg;
        int expectedBadMsgCount = 0;

        CountDownLatch countIntervalTopicsLatch = startIntervalTopicsConsumer(
                StatisticType.COUNT,
                consumerProps,
                expectedGoodMsgCountPerStatType,
                topicToMsgsMap,
                true,
                100);
        CountDownLatch valueIntervalTopicsLatch = startIntervalTopicsConsumer(
                StatisticType.VALUE,
                consumerProps,
                expectedGoodMsgCountPerStatType,
                topicToMsgsMap,
                true,
                100);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);
        startInputEventsConsumer(consumerProps);

        ThreadUtil.sleepAtLeastIgnoreInterrupts(2_000);

        for (StatisticType statisticType : StatisticType.values()) {
            String inputTopic = INPUT_TOPICS_MAP.get(statisticType);

            for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
                //stat name == topic name
                String statName = TopicNameFactory.getIntervalTopicName(
                        STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX,
                        statisticType,
                        interval);
                String statUuid = StatisticsHelper.getUuidKey(statName);

                String tag1 = TopicNameFactory.getIntervalTopicName("tag1", statisticType, interval);
                String tag2 = TopicNameFactory.getIntervalTopicName("tag2", statisticType, interval);

                addStatConfig(module.getMockStatisticConfigurationService(),
                        statUuid,
                        statName,
                        statisticType,
                        Arrays.asList(tag1, tag2),
                        interval);

                Statistics statistics;
                if (statisticType.equals(StatisticType.COUNT)) {
                    statistics = StatisticsHelper.buildStatistics(
                            StatisticsHelper.buildCountStatistic(statUuid, statName, time, 1L,
                                    StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                                    StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                            )
                    );
                } else {
                    statistics = StatisticsHelper.buildStatistics(
                            StatisticsHelper.buildValueStatistic(statUuid, statName, time, 1.5,
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
        producer.close();

        //Wait for the expected numbers of messages to arrive or timeout if not

        SoftAssertions.assertSoftly(softly -> {
            try {
                softly.assertThat(countIntervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
                softly.assertThat(valueIntervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
                softly.assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
                softly.assertThat(topicToMsgsMap).hasSize(expectedTopicCount);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(String.format("Thread interrupted"), e);
            }
        });

        topicToMsgsMap.entrySet().forEach(entry -> assertThat(entry.getValue()).hasSize(expectedPermsPerMsg));
        //make sure the messages have gone to the correct topic by comparing their statName to the topic name
        topicToMsgsMap.entrySet().forEach(entry -> {
            UID statUuidUid = entry.getValue().stream()
                    .findFirst()
                    .get()
                    .key()
                    .getStatUuid();
            String statUuid = uniqueIdCache.getName(statUuidUid);
            String statName = StatisticsHelper.getStatName(statUuid);

            String topicName = entry.getKey();
            assertThat(statName).isEqualTo(topicName);
        });

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

    @Test
    public void test_cantUnmarshall() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;

        addStatConfig(module.getMockStatisticConfigurationService(),
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                interval);

        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                //the good, at this point
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, time, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "earliest");

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        int expectedBadMsgCount = 1;

        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);


        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        String statKey = statistics.getStatistic().get(0).getKey().getValue();
        //corrupt the xml by renaming one of the element names
        String msgValue = statisticsMarshaller.marshallToXml(statistics)
                .replaceAll("key", "badElementName");
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, statKey, msgValue);
        producer.send(producerRecord).get();
        producer.close();

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();

        assertThat(badEvents)
                .hasSize(expectedBadMsgCount);
        assertThat(badEvents.values().stream().findFirst().get())
                .hasSize(expectedBadMsgCount);
        assertThat(badEvents.values().stream().findFirst().get().stream().findFirst().get())
                .contains(GOOD_STAT_NAME);
        assertThat(badEvents.values().stream().findFirst().get().stream().findFirst().get())
                .contains(StatisticsFlatMappingStreamFactory.UNMARSHALLING_ERROR_TEXT);
    }
    @Test
    public void test_oneGoodOneBad() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);
        String badStatName = "badStatName";
        String badStatUuid = StatisticsHelper.getUuidKey("badStatName");


        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;

        addStatConfig(module.getMockStatisticConfigurationService(),
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                interval);

        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                //the good
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, time, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                //the bad
                StatisticsHelper.buildCountStatistic(badStatUuid, badStatName, time.plusHours(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "earliest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //1 good input msgs, each one is rolled up to 4 perms so expect 4
        int expectedGoodMsgCount = 4;
        int expectedBadMsgCount = 1;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(
                StatisticType.COUNT,
                consumerProps,
                expectedGoodMsgCount,
                topicToMsgsMap,
                true,
                100);

        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);
//        startInputEventsConsumer(consumerProps);


        //give the consumers and streams enough time to spin up
//        ThreadUtil.sleepAtLeastIgnoreInterrupts(1_000);

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();
        producer.close();

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();

        //only one interval topic
        assertThat(topicToMsgsMap).hasSize(1);
        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatEventKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
        assertThat(messages).hasSize(expectedGoodMsgCount);

        //no bad events
        assertThat(badEvents)
                .hasSize(expectedBadMsgCount);
        assertThat(badEvents.values().stream().findFirst().get())
                .hasSize(expectedBadMsgCount);
        assertThat(badEvents.values().stream().findFirst().get().stream().findFirst().get())
                .contains(badStatName);
        assertThat(badEvents.values().stream().findFirst().get().stream().findFirst().get())
                .contains(StatisticsFlatMappingStreamFactory.VALIDATION_ERROR_TEXT);
    }

    @Test
    public void test_oneEventOutsideBiggestRetentionOneInside() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            //one row interval for DAY is 52wks
            setPurgeRetention(interval, 1);
        }

        ZonedDateTime timeNow = ZonedDateTime.now(ZoneOffset.UTC);

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);


        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.DAY;

        addStatConfig(module.getMockStatisticConfigurationService(),
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                interval);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, timeNow, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                //Event time is 2 years ago so will be outside all purge retention thresholds
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, timeNow.minusYears(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();
        producer.close();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put("auto.offset.reset", "earliest");

//        startAllTopicsConsumer(consumerProps);

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //1 good input msg, each one is rolled up to 4 perms so expect 4
        int expectedGoodMsgCount = 4;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCount, topicToMsgsMap, true, 100);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //Wait for the expected numbers of messages to arrive or timeout if not
        assertThat(intervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();

        //Only the good event goes on the topic, the one outside the retention is just ignored
        assertThat(topicToMsgsMap).hasSize(1);
        String topicName = topicToMsgsMap.keySet().stream().findFirst().get();
        assertThat(topicName).isEqualTo(TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
        List<ConsumerRecord<StatEventKey, StatAggregate>> messages = topicToMsgsMap.values().stream().findFirst().get();
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
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(
                senderProps,
                Serdes.String().serializer(),
                Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            //one row key interval (see EventStoreTimeIntervalEnum for details) of retention
            setPurgeRetention(interval, 1);
        }

        ZonedDateTime timeNow = ZonedDateTime.now(ZoneOffset.UTC);

        StatisticType statisticType = StatisticType.COUNT;
        String topic = INPUT_TOPICS_MAP.get(statisticType);

        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.SECOND;

        addStatConfig(module.getMockStatisticConfigurationService(),
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                interval);

        Statistics statistics = StatisticsHelper.buildStatistics(
                //bump up to MIN interval
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, timeNow.minusHours(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                //bumped up to HOUR interval
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, timeNow.minusDays(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                //bumped up to DAY interval
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, timeNow.minusWeeks(8), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                //ignored
                StatisticsHelper.buildCountStatistic(GOOD_STAT_UUID, GOOD_STAT_NAME, timeNow.minusYears(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        //3 good input msg, each one is rolled up to 4 perms so expect 12, one input msg ignored
        int expectedGoodMsgCount = 3 * 4;
        int expectedBadMsgCount = 0;
        CountDownLatch intervalTopicsLatch = startIntervalTopicsConsumer(
                StatisticType.COUNT,
                consumerProps,
                expectedGoodMsgCount,
                topicToMsgsMap,
                true,
                100);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);
//        startAllTopicsConsumer(consumerProps);

        //Allow a bit of time for the consumers to fire up
        ThreadUtil.sleepAtLeastIgnoreInterrupts(1_000);

        LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), topic);
        producer.send(buildProducerRecord(topic, statistics)).get();

        SoftAssertions.assertSoftly(softly -> {
            try {
                //Wait for the expected numbers of messages to arrive or timeout if not
                softly.assertThat(intervalTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
                softly.assertThat(badTopicsLatch.await(30, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(String.format("Thread interrupted"), e);
            }

            //Only the good event goes on the topic, the one outside the retention is just ignored
            softly.assertThat(topicToMsgsMap).hasSize(3);
            Set<String> topicNames = topicToMsgsMap.keySet();
            softly.assertThat(topicNames).contains(
                    TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, EventStoreTimeIntervalEnum.MINUTE),
                    TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, EventStoreTimeIntervalEnum.HOUR),
                    TopicNameFactory.getIntervalTopicName(STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, EventStoreTimeIntervalEnum.DAY)
            );
            topicToMsgsMap.values().forEach(val ->
                    softly.assertThat(val).hasSize(4)
            );

            //no bad events
            softly.assertThat(badEvents).hasSize(expectedBadMsgCount);
        });
    }

    @Ignore //covered by FullEndToEndVolumeIT
    @Test
    public void test_ManyEventsOnMultipleThreads() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        setNumStreamThreads(4);
        module = initStreamProcessing();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps, Serdes.String().serializer(), Serdes.String().serializer());

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            setPurgeRetention(interval, 10_000);
        }
        setAggregationProps(10_000, 4_000, 100);

        int statNameCnt = 10;
        int msgCntPerStatNameAndIntervalAndType = 100;

        List<ProducerRecord<String, String>> producerRecords = new ArrayList<>();

        long counter = Instant.now().toEpochMilli();

        Instant baseInstant = Instant.now().truncatedTo(ChronoUnit.DAYS);

//        StatisticType[] types = new StatisticType[] {StatisticType.COUNT};
        StatisticType[] types = StatisticType.values();
//        EventStoreTimeIntervalEnum[] intervals = new EventStoreTimeIntervalEnum[]{EventStoreTimeIntervalEnum.SECOND};
        EventStoreTimeIntervalEnum[] intervals = EventStoreTimeIntervalEnum.values();

        for (StatisticType statisticType : types) {
            String inputTopic = INPUT_TOPICS_MAP.get(statisticType);

            for (EventStoreTimeIntervalEnum interval : intervals) {
//                EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;

                int cnt = 0;

                for (int statNum : IntStream.rangeClosed(1, statNameCnt).toArray()) {
                    String statName = TopicNameFactory.getIntervalTopicName("MyStat-" + statNum, statisticType, interval);
                    String statUuid = StatisticsHelper.getUuidKey(statName);

                    String tag1 = "tag1-" + statName;
                    String tag2 = "tag2-" + statName;

                    addStatConfig(module.getMockStatisticConfigurationService(),
                            statUuid,
                            statName,
                            statisticType,
                            Arrays.asList(tag1, tag2),
                            interval);

                    for (int i : IntStream.rangeClosed(1, msgCntPerStatNameAndIntervalAndType).toArray()) {

                        Random random = new Random();
                        //Give each source event a different time to aid debugging
//                        ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(counter), ZoneOffset.UTC);

                        //each event within a statUuid/interval/type combo has a time two days apart to ensure no aggregation
                        ZonedDateTime time = ZonedDateTime.ofInstant(baseInstant, ZoneOffset.UTC).plusDays(i * 2);
                        Statistics statistics;
                        if (statisticType.equals(StatisticType.COUNT)) {
                            statistics = StatisticsHelper.buildStatistics(
                                    StatisticsHelper.buildCountStatistic(statUuid, statName, time, 1,
                                            StatisticsHelper.buildTagType(tag1, tag1 + "val" + random.nextInt(3)),
                                            StatisticsHelper.buildTagType(tag2, tag2 + "val" + random.nextInt(3))
                                    )
                            );
                        } else {
                            statistics = StatisticsHelper.buildStatistics(
                                    StatisticsHelper.buildValueStatistic(statUuid, statName, time, 1.0,
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


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dummyGroup", "false", kafkaEmbedded);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap = new ConcurrentHashMap<>();
        Map<String, List<String>> badEvents = new HashMap<>();

        int expectedTopicsPerStatType = intervals.length;
        int expectedTopicCount = types.length * expectedTopicsPerStatType;
        int expectedPermsPerMsg = 4; //based on two tags so a rollup type of ALL gives 4 perms
        int expectedGoodMsgCountPerStatTypeAndInterval = statNameCnt *
                expectedPermsPerMsg * msgCntPerStatNameAndIntervalAndType;
//        int expectedGoodMsgCountPerStatType = intervals.length * expectedGoodMsgCountPerStatTypeAndInterval;
        Map<EventStoreTimeIntervalEnum, Integer> expectedCountPerIntervalAndTypeMap = getExpectedCountPerStatType(
                expectedGoodMsgCountPerStatTypeAndInterval, intervals);

        int expectedGoodMsgCountPerStatType = expectedCountPerIntervalAndTypeMap.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
        int expectedBadMsgCount = 0;

        LOGGER.info("Expecting {} msgs per stat type and interval, {} per stat type",
                expectedGoodMsgCountPerStatTypeAndInterval, expectedGoodMsgCountPerStatType);

        CountDownLatch countIntervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.COUNT, consumerProps, expectedGoodMsgCountPerStatType, topicToMsgsMap, true, 10_000);
        CountDownLatch valueIntervalTopicsLatch = startIntervalTopicsConsumer(StatisticType.VALUE, consumerProps, expectedGoodMsgCountPerStatType, topicToMsgsMap, true, 10_000);
        CountDownLatch badTopicsLatch = startBadEventsConsumer(consumerProps, expectedBadMsgCount, badEvents);

        //use multiple threads to send the messages asynchronously
        producerRecords
//                .parallelStream()
                .stream()
                .forEach(rec -> {
                    try {
                        @SuppressWarnings("FutureReturnValueIgnored")
                        Future<RecordMetadata> future = producer.send(rec, (metadata, ex) -> {
                            if (ex != null) {
                                LOGGER.error("Error sending to kafka", ex);
                                Assert.fail();
                            }
                        });
                    } catch (Exception e) {
                        throw new RuntimeException("exception sending mesg", e);
                    }
                });

        @SuppressWarnings("FutureReturnValueIgnored")
        Future<?> future = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                StringBuilder sb = new StringBuilder("Current state of the topicToMsgsMap:\n");
                topicToMsgsMap.keySet().stream()
                        .sorted()
                        .forEach(key -> sb.append(key + " - " + topicToMsgsMap.get(key).size() + "\n"));
                LOGGER.debug(sb.toString());
                LOGGER.debug("Count latch {}, Value latch {}", countIntervalTopicsLatch.getCount(), valueIntervalTopicsLatch.getCount());
            } catch (Exception e) {
                LOGGER.error("Got error in executor: {}", e.getMessage(), e);
            }
        }, 0, 2, TimeUnit.SECONDS);

        producer.close();


        //Wait for the expected numbers from messages to arrive or timeout if not
        ;
        ;
        assertThat(badTopicsLatch.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(countIntervalTopicsLatch.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(valueIntervalTopicsLatch.await(60, TimeUnit.SECONDS)).isTrue();

        assertThat(topicToMsgsMap).hasSize(expectedTopicCount);

        //If we put N recs into S/M/H/D stores then the S->M, M->H, H->D so expect S=N, M=2N, H=3N, D=4N
        assertThat(topicToMsgsMap.entrySet().stream()
                .filter(entry -> entry.getKey().contains("Count"))
                .map(entry -> entry.getValue().size())
                .collect(Collectors.toList())
        ).containsExactlyInAnyOrder(
                expectedCountPerIntervalAndTypeMap.values()
                        .toArray(new Integer[expectedCountPerIntervalAndTypeMap.size()])
        );

        assertThat(topicToMsgsMap.entrySet().stream()
                .filter(entry -> entry.getKey().contains("Value"))
                .map(entry -> entry.getValue().size())
                .collect(Collectors.toList())
        ).containsExactlyInAnyOrder(
                expectedCountPerIntervalAndTypeMap.values()
                        .toArray(new Integer[expectedCountPerIntervalAndTypeMap.size()])
        );

        //no bad events
        assertThat(badEvents).hasSize(expectedBadMsgCount);
    }

    /**
     * Based on the provided array of intervals and the expected count per interval, work out
     * the expected count for each taking into account that records bubble up the interval chain,
     * i.e. S->M, M->H, etc.
     */
    private Map<EventStoreTimeIntervalEnum, Integer> getExpectedCountPerStatType(
            final int expectedCountPerStatTypeAndInterval,
            final EventStoreTimeIntervalEnum[] intervals) {

        Map<EventStoreTimeIntervalEnum, Integer> expectedCountsMap = new HashMap<>();
        Arrays.stream(intervals).forEach(interval -> expectedCountsMap.put(interval, 0));

        Arrays.stream(intervals).forEach(interval ->
                addExpectedCount(expectedCountPerStatTypeAndInterval, interval, expectedCountsMap));
        return expectedCountsMap;
    }


    /**
     * Recursive function to walk up each iteration of time intervals until the biggest calculating the
     * expected count for each interval along the way
     */
    private void addExpectedCount(int expectedCount, EventStoreTimeIntervalEnum currInterval, Map<EventStoreTimeIntervalEnum, Integer> countsMap) {
        countsMap.merge(currInterval, expectedCount, (v1, v2) -> v1 + v2);

        EventStoreTimeIntervalHelper.getNextBiggest(currInterval)
                .ifPresent(nextInterval -> addExpectedCount(expectedCount, nextInterval, countsMap));
    }

    /**
     * Start a consumer consuming from all time interval topics (one for each interval of the passed stat type so 4 in all)
     * and both log each message and add the message to a map keyed by topic name.
     * A {@link CountDownLatch} is returned to allow the caller to wait for the expected number of messages
     */
    private CountDownLatch startIntervalTopicsConsumer(final StatisticType statisticType,
                                                       final Map<String, Object> consumerProps,
                                                       final int expectedMsgCount,
                                                       final ConcurrentMap<String, List<ConsumerRecord<StatEventKey, StatAggregate>>> topicToMsgsMap,
                                                       final boolean isEachMsgLogged,
                                                       final int pollIntervalMs) {

        String groupId = "consumer-" + statisticType;
        Map<String, Object> consumerPropsLocal = KafkaTestUtils.consumerProps(groupId, "false", kafkaEmbedded);
//        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5_000);
        consumerPropsLocal.putAll(consumerProps);

        final CountDownLatch latch = new CountDownLatch(expectedMsgCount);
        final Serde<StatAggregate> stagAggSerde = StatAggregateSerde.instance();
        final Serde<StatEventKey> statKeySerde = StatEventKeySerde.instance();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            Thread.currentThread().setName("int-csmr-" + statisticType + "-thrd");
            activeConsumerThreads.incrementAndGet();
            KafkaConsumer<StatEventKey, StatAggregate> kafkaConsumer = new KafkaConsumer<>(consumerPropsLocal,
                    statKeySerde.deserializer(),
                    stagAggSerde.deserializer());

            List<String> topics = ROLLUP_TOPICS_MAP.get(statisticType);
            LOGGER.info("Starting consumer for type {}, expectedMsgCount {}, topics {}",
                    statisticType, expectedMsgCount, topics);
            kafkaConsumer.subscribe(topics);

            AtomicLong recCounter = new AtomicLong(0);

            try {
                while (areConsumersEnabled.get()) {
                    try {
                        ConsumerRecords<StatEventKey, StatAggregate> records = kafkaConsumer.poll(pollIntervalMs);
                        if (records.count() > 0) {
//                            recCounter.addAndGet(records.count());
                            LOGGER.trace("{} consumed {} good records, cumulative count {}", statisticType, records.count(), recCounter.get());
                            for (ConsumerRecord<StatEventKey, StatAggregate> record : records) {
                                try {
                                    if (isEachMsgLogged) {
                                        LOGGER.trace("IntervalTopicsConsumer - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                                        if (LOGGER.isTraceEnabled()) {
                                            StatEventKeyUtils.logStatKey(uniqueIdCache, record.key());
                                        }
                                    }
                                    topicToMsgsMap.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record);
                                    latch.countDown();
                                } catch (Exception e) {
                                    LOGGER.error("Error processing record {}, {}", record, e.getMessage(), e);
                                }
                            }
                            LOGGER.trace("Committing consumer [{}]", statisticType);
                            kafkaConsumer.commitSync();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error while polling with stat type {}", statisticType, e);
                    }
                    if (latch.getCount() == 0) {
                        break;
                    }
                }
            } finally {
                LOGGER.debug("Closing interval consumer [{}]", statisticType);
                kafkaConsumer.close();
                activeConsumerThreads.decrementAndGet();
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
            Thread.currentThread().setName("bad-events-consumer-thread");
            activeConsumerThreads.incrementAndGet();
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());

            //Subscribe to all bad event topics
            kafkaConsumer.subscribe(BAD_TOPICS_MAP.values());

            try {
                while (areConsumersEnabled.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    if (records.count() > 0) {
                        for (ConsumerRecord<String, String> record : records) {
                            LOGGER.warn("Bad events Consumer - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
                            messages.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record.value());
                            latch.countDown();
                        }
                        if (latch.getCount() == 0) {
                            break;
                        }
                        kafkaConsumer.commitSync();
                    }
                }
            } finally {
                kafkaConsumer.close();
                activeConsumerThreads.decrementAndGet();
            }
        });
        return latch;
    }

    private void startInputEventsConsumer(final Map<String, Object> consumerProps) {

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            activeConsumerThreads.incrementAndGet();
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());

            //Subscribe to all input event topics
            kafkaConsumer.subscribe(INPUT_TOPICS_MAP.values());

            try {
                while (areConsumersEnabled.get()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.info("Input events Consumer - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                }
            } finally {
                kafkaConsumer.close();
                activeConsumerThreads.decrementAndGet();
            }
        });
    }

    private void addStatConfig(MockStatisticConfigurationService mockStatisticConfigurationService,
                               String statUuid,
                               String statName,
                               StatisticType statisticType,
                               List<String> fieldNames,
                               EventStoreTimeIntervalEnum precision) {
        MockStatisticConfiguration statConfig = new MockStatisticConfiguration()
                .setUuid(statUuid)
                .setName(statName)
                .setStatisticType(statisticType)
                .setRollUpType(StatisticRollUpType.ALL)
                .addFieldNames(fieldNames)
                .setPrecision(precision);

        LOGGER.debug("Adding StatConfig: {} {} {} {} {} {}",
                statConfig.getUuid(),
                statConfig.getName(),
                statConfig.getStatisticType(),
                statConfig.getRollUpType(),
                statConfig.getFieldNames(),
                statConfig.getPrecision());

        mockStatisticConfigurationService.addStatisticConfiguration(statConfig);
    }


    private ProducerRecord<String, String> buildProducerRecord(String topic, Statistics statistics) {
        String statKey = statistics.getStatistic().get(0).getKey().getValue();
        return new ProducerRecord<>(topic, statKey, statisticsMarshaller.marshallToXml(statistics));
    }

    private static KafkaEmbedded buildEmbeddedKafka() {
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


//        topics.forEach(topic -> LOGGER.info("Creating topic: {}", topic));

//        return new KafkaEmbedded(1, true, 1, topics.toArray(new String[topics.size()]));
        return new KafkaEmbedded(1, true, 1);
    }

    private StroomStatsEmbeddedOverrideModule initStreamProcessing() {
        //Set up the properties service so it points to the embedded kafka
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaEmbedded);
        String zookeeprConnectStr = kafkaEmbedded.getZookeeperConnectionString();

        String bootStrapServersConfig = (String) senderProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        //Override the kafka bootstrap servers prop to point to the embedded kafka rather than the docker one
        mockStroomPropertyService.setProperty(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS, bootStrapServersConfig);
        mockStroomPropertyService.setProperty(StatisticsIngestService.PROP_KEY_KAFKA_COMMIT_INTERVAL_MS, 100);

        //set to earliest so that it will process data added before the kstreams are started
        mockStroomPropertyService.setProperty(StatisticsIngestService.PROP_KEY_KAFKA_AUTO_OFFSET_RESET, "earliest");
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 1_000);

        SessionFactory mockSessionFactory = Mockito.mock(SessionFactory.class);
        StatisticsService mockStatisticsService = Mockito.mock(StatisticsService.class);

        StroomStatsEmbeddedOverrideModule embeddedOverrideModule = new StroomStatsEmbeddedOverrideModule(
                mockStroomPropertyService,
                Optional.of(mockStatisticsService));


        ZookeeperConfig mockZookeeperConfig = Mockito.mock(ZookeeperConfig.class);
        Config mockConfig = Mockito.mock(Config.class);
        Mockito.when(mockZookeeperConfig.getQuorum()).thenReturn(kafkaEmbedded.getZookeeperConnectionString());
        Mockito.when(mockZookeeperConfig.getPropertyServicePath()).thenReturn("/propertyService");
        Mockito.when(mockZookeeperConfig.getServiceDiscoveryPath()).thenReturn("/stroom-services");
        Mockito.when(mockConfig.getZookeeperConfig()).thenReturn(mockZookeeperConfig);


        //override the service guice module with our test one that uses mocks for props, stat config and stats service
        Module embeddedServiceModule = Modules
                .override(new StroomStatsServiceModule(mockConfig, mockSessionFactory))
                .with(embeddedOverrideModule);

        injector = Guice.createInjector(embeddedServiceModule);

        //get an instance of the kafkaStreamService so we know it has started up
        injector.getInstance(StatisticsIngestService.class);

        uniqueIdCache = injector.getInstance(UniqueIdCache.class);

        injector.getInstance(StatisticsFlatMappingService.class).start();

        return embeddedOverrideModule;
    }

    private void dumpStatistics(Statistics statisticsObj) {
        if (LOGGER.isTraceEnabled()) {
            statisticsObj.getStatistic().forEach(statistic -> {
                String tagValues = statistic.getTags().getTag().stream()
                        .map(tagValue -> tagValue.getName() + "|" + tagValue.getValue())
                        .collect(Collectors.joining(","));
                LOGGER.trace("Stat: {} {} {} {} {} {}",
                        statistic.getKey().getValue(),
                        statistic.getKey().getStatisticName(),
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
                StatisticsIngestService.PROP_KEY_KAFKA_STREAM_THREADS, newValue);
    }

    private void setAggregationProps(final int minBatchSize, final int maxFlushIntervalMs, final int pollTimeoutMs) {
        mockStroomPropertyService.setProperty(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, minBatchSize);
        mockStroomPropertyService.setProperty(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, maxFlushIntervalMs);
        mockStroomPropertyService.setProperty(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS, pollTimeoutMs);
    }

    private String getAggregateValue(StatAggregate statAggregate) {
        if (statAggregate instanceof CountAggregate) {
            return Long.toString(((CountAggregate) statAggregate).getAggregatedCount());
        } else if (statAggregate instanceof ValueAggregate) {
            return Double.toString(((ValueAggregate) statAggregate).getAggregatedValue());
        } else {
            throw new RuntimeException(String.format("Unexpected type %s", statAggregate.getClass().getName()));
        }
    }


}