package stroom.stats.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.api.StatisticType;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.uid.UID;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.v4.Statistics;
import stroom.stats.schema.v4.StatisticsMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.streams.mapping.StatisticFlatMapper;
import stroom.stats.streams.topics.TopicDefinition;
import stroom.stats.streams.topics.TopicDefinitionFactory;
import stroom.stats.test.StatisticsHelper;

import javax.xml.datatype.DatatypeConfigurationException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatisticsFlatMappingProcessor extends AbstractStreamProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestStatisticsFlatMappingProcessor.class);

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

    private static final int ROLLUPS_PER_STAT = 2;

    private final MockStatisticConfigurationService mockStatisticConfigurationService = new MockStatisticConfigurationService();
    private final MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();
    private final TopicDefinitionFactory topicDefinitionFactory = new TopicDefinitionFactory(mockStroomPropertyService);
    private final StatisticsMarshaller statisticsMarshaller = new StatisticsMarshaller();

    @Test
    public void test_twoGoodCountEvents() {

        final StatisticType statisticType = StatisticType.COUNT;

        final ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);
        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(time, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                StatisticsHelper.buildCountStatistic(time.plusDays(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );

        doSimpleSingleIntervalTest(statisticType, statistics);
    }

    @Test
    public void test_twoGoodValueEvents() {

        final StatisticType statisticType = StatisticType.VALUE;

        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildValueStatistic(time, 1.5,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                StatisticsHelper.buildValueStatistic(time.plusHours(2), 1.5,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );

        doSimpleSingleIntervalTest(statisticType, statistics);
    }

    @Test
    public void test_OneGoodEventPerInterval_Count() {

        final StatisticType statisticType = StatisticType.COUNT;

        final TopicDefinition<String, String> statEventsTopic = topicDefinitionFactory.getStatisticEventsTopic(statisticType);
        final TopicDefinition<String, String> badStatEventsTopic = topicDefinitionFactory.getBadStatisticEventsTopic(statisticType);
        final EventStoreTimeIntervalEnum precisionInterval = EventStoreTimeIntervalEnum.MINUTE;

        final TopicDefinition<StatEventKey, StatAggregate> expectedOutputTopic = topicDefinitionFactory.getAggregatesTopic(
                statisticType, precisionInterval);

        final StreamProcessor streamProcessor = buildStreamProcessor(statisticType);

        int expectedTopicCount = EventStoreTimeIntervalEnum.values().length;
        int expectedPermsPerMsg = ROLLUPS_PER_STAT;
//        int expectedGoodMsgCountPerTopic = expectedTopicCount * expectedPermsPerMsg;
//        int expectedBadMsgCount = 0;

        final ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);


        runProcessorTest(statEventsTopic, streamProcessor, (testDriver, consumerRecordFactory) -> {
            for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
                //stat name == topic name
                String statName = TopicNameFactory.getIntervalTopicName(
                        STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX,
                        statisticType,
                        interval);
                String statUuid = StatisticsHelper.getUuidKey(statName);

                String tag1 = TopicNameFactory.getIntervalTopicName("tag1", statisticType, interval);
                String tag2 = TopicNameFactory.getIntervalTopicName("tag2", statisticType, interval);

                addStatConfig(mockStatisticConfigurationService,
                        statUuid,
                        statName,
                        statisticType,
                        Arrays.asList(tag1, tag2),
                        interval);

                Statistics statistics;
                if (statisticType.equals(StatisticType.COUNT)) {
                    statistics = StatisticsHelper.buildStatistics(
                            StatisticsHelper.buildCountStatistic(time, 1L,
                                    StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                                    StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                            )
                    );
                } else {
                    statistics = StatisticsHelper.buildStatistics(
                            StatisticsHelper.buildValueStatistic(time, 1.5,
                                    StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                                    StatisticsHelper.buildTagType(tag2, tag2 + "val1")
                            )
                    );
                }
                dumpStatistics(statistics);
                LOGGER.info("Sending to {} stat events to topic {}", statistics.getStatistic().size(), statEventsTopic);
                sendStatistics(testDriver, consumerRecordFactory, statUuid, statistics);
            }

            TopicDefinition<StatEventKey, StatAggregate>[] expectedOutputTopics = topicDefinitionFactory.getAggregateTopicsMap()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getKey()._1().equals(statisticType))
                    .map(Map.Entry::getValue)
                    .toArray(TopicDefinition[]::new);

            final Map<TopicDefinition<StatEventKey, StatAggregate>, List<ProducerRecord<StatEventKey, StatAggregate>>>
                    topicToAggregatesMap = getAndAssertOutputAggregates(testDriver, expectedPermsPerMsg, expectedOutputTopics);

            getAndAssertBadEvents(testDriver, 0);
        });
    }

    @Test
    public void test_cantUnmarshall() throws ExecutionException, InterruptedException, DatatypeConfigurationException {
        final StatisticType statisticType = StatisticType.COUNT;
        final TopicDefinition<String, String> statEventsTopic = topicDefinitionFactory.getStatisticEventsTopic(statisticType);
        final TopicDefinition<String, String> badStatEventsTopic = topicDefinitionFactory.getBadStatisticEventsTopic(statisticType);
        final EventStoreTimeIntervalEnum precisionInterval = EventStoreTimeIntervalEnum.MINUTE;

        final StreamProcessor streamProcessor = buildStreamProcessor(statisticType);

        final ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);
        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(time, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                StatisticsHelper.buildCountStatistic(time.plusDays(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );

        addStatConfig(mockStatisticConfigurationService,
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                precisionInterval);

        runProcessorTest(statEventsTopic, streamProcessor, (testDriver, consumerRecordFactory) -> {

            String msgValue = statisticsMarshaller.marshallToXml(statistics)
                    .replaceAll("statistic", "badElementName");

            super.sendMessage(testDriver, consumerRecordFactory, GOOD_STAT_UUID, msgValue);

            getAndAssertOutputAggregates(testDriver, 0);

            final Map<TopicDefinition<String, String>, List<ProducerRecord<String, String>>> badEvents =
                    getAndAssertBadEvents(testDriver, 1, badStatEventsTopic);

            assertThat(badEvents.values().stream().flatMap(List::stream).map(ProducerRecord::value).findFirst().get())
                    .contains(StatisticsFlatMappingStreamFactory.UNMARSHALLING_ERROR_TEXT);
        });
    }

    @Test
    public void test_oneGoodOneBad() {
        final StatisticType statisticType = StatisticType.COUNT;
        final TopicDefinition<String, String> statEventsTopic = topicDefinitionFactory.getStatisticEventsTopic(statisticType);
        final TopicDefinition<String, String> badStatEventsTopic = topicDefinitionFactory.getBadStatisticEventsTopic(statisticType);
        final EventStoreTimeIntervalEnum precisionInterval = EventStoreTimeIntervalEnum.MINUTE;

        final TopicDefinition<StatEventKey, StatAggregate> expectedOutputTopic = topicDefinitionFactory.getAggregatesTopic(
                statisticType, precisionInterval);

        final StreamProcessor streamProcessor = buildStreamProcessor(statisticType);

        final String badStatName = "badStatName";
        final String badStatUuid = StatisticsHelper.getUuidKey("badStatName");

        final ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);

        Statistics statisticsGood = StatisticsHelper.buildStatistics(
                //the good
                StatisticsHelper.buildCountStatistic(time, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ));

        Statistics statisticsBad = StatisticsHelper.buildStatistics(
                //the bad
                StatisticsHelper.buildCountStatistic(time.plusHours(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ));

        addStatConfig(mockStatisticConfigurationService,
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                precisionInterval);

        runProcessorTest(statEventsTopic, streamProcessor, (testDriver, consumerRecordFactory) -> {

            sendStatistics(testDriver, consumerRecordFactory, GOOD_STAT_UUID, statisticsGood);
            sendStatistics(testDriver, consumerRecordFactory, badStatUuid, statisticsBad);

            getAndAssertOutputAggregates(testDriver, ROLLUPS_PER_STAT, expectedOutputTopic);

            getAndAssertBadEvents(testDriver, 1, badStatEventsTopic);
        });
    }

    @Test
    public void test_oneEventOutsideBiggestRetentionOneInside() {
        final StatisticType statisticType = StatisticType.COUNT;
        final EventStoreTimeIntervalEnum precisionInterval = EventStoreTimeIntervalEnum.DAY;
        final TopicDefinition<String, String> statEventsTopic = topicDefinitionFactory.getStatisticEventsTopic(statisticType);
        final TopicDefinition<String, String> badStatEventsTopic = topicDefinitionFactory.getBadStatisticEventsTopic(statisticType);
        final TopicDefinition<StatEventKey, StatAggregate> expectedOutputTopic = topicDefinitionFactory.getAggregatesTopic(
                statisticType, precisionInterval);
        final StreamProcessor streamProcessor = buildStreamProcessor(statisticType);

        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            //one row interval for DAY is 52wks
            setPurgeRetention(interval, 1);
        }

        ZonedDateTime timeNow = ZonedDateTime.now(ZoneOffset.UTC);

        addStatConfig(mockStatisticConfigurationService,
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                precisionInterval);

        Statistics statistics = StatisticsHelper.buildStatistics(
                StatisticsHelper.buildCountStatistic(timeNow, 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                ),
                // Event time is 2 years ago so will be outside all purge retention thresholds
                // and thus will be filtered out
                StatisticsHelper.buildCountStatistic(timeNow.minusYears(2), 1L,
                        StatisticsHelper.buildTagType(TAG_1, TAG_1 + "val1"),
                        StatisticsHelper.buildTagType(TAG_2, TAG_2 + "val1")
                )
        );

        runProcessorTest(statEventsTopic, streamProcessor, (testDriver, consumerRecordFactory) -> {
            sendStatistics(testDriver, consumerRecordFactory, GOOD_STAT_UUID, statistics);
            getAndAssertOutputAggregates(testDriver, ROLLUPS_PER_STAT, expectedOutputTopic);
        });
    }

    private void setPurgeRetention(final EventStoreTimeIntervalEnum interval, final int newValue) {
        mockStroomPropertyService.setProperty(
                HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX +
                        interval.name().toLowerCase(), newValue);
    }


    private void doSimpleSingleIntervalTest(final StatisticType statisticType, Statistics inputStatistics) {
        final TopicDefinition<String, String> statEventsTopic = topicDefinitionFactory.getStatisticEventsTopic(statisticType);
        final TopicDefinition<String, String> badStatEventsTopic = topicDefinitionFactory.getBadStatisticEventsTopic(statisticType);
        final EventStoreTimeIntervalEnum precisionInterval = EventStoreTimeIntervalEnum.MINUTE;

        final TopicDefinition<StatEventKey, StatAggregate> expectedOutputTopic = topicDefinitionFactory.getAggregatesTopic(
                statisticType, precisionInterval);

        final StreamProcessor streamProcessor = buildStreamProcessor(statisticType);

        addStatConfig(mockStatisticConfigurationService,
                GOOD_STAT_UUID,
                GOOD_STAT_NAME,
                statisticType,
                Arrays.asList(TAG_1, TAG_2),
                precisionInterval);

        runProcessorTest(statEventsTopic, streamProcessor, (testDriver, consumerRecordFactory) -> {

            sendStatistics(testDriver, consumerRecordFactory, GOOD_STAT_UUID, inputStatistics);

            int expectedAggregateRecordCount = inputStatistics.getStatistic().size() * ROLLUPS_PER_STAT;
            getAndAssertOutputAggregates(testDriver, expectedAggregateRecordCount, expectedOutputTopic);

            getAndAssertBadEvents(testDriver, 0);
        });
    }

    private Map<TopicDefinition<String, String>, List<ProducerRecord<String, String>>> getAndAssertBadEvents(
            final TopologyTestDriver testDriver,
            final int expectedCountInDesiredTopic,
            final TopicDefinition<String, String>... expectedTopics) {

        final Map<TopicDefinition<String, String>, List<ProducerRecord<String, String>>> outputRecordsMap =
                super.readAllProducerRecords(
                        topicDefinitionFactory.getBadStatisticEventTopicsMap().values(),
                        testDriver);

        assertThat(outputRecordsMap.keySet())
                .contains(expectedTopics);

        outputRecordsMap.forEach((topicDef, producerRecord) -> {

            // only expecting output in one topic
                assertThat(outputRecordsMap.get(topicDef))
                        .hasSize(expectedCountInDesiredTopic);
        });
        return outputRecordsMap;
    }


    private Map<TopicDefinition<StatEventKey, StatAggregate>, List<ProducerRecord<StatEventKey, StatAggregate>>> getAndAssertOutputAggregates(
            final TopologyTestDriver testDriver,
            final int expectedCountInDesiredTopic,
            final TopicDefinition<StatEventKey, StatAggregate>... expectedOutputTopics) {

        final Map<TopicDefinition<StatEventKey, StatAggregate>, List<ProducerRecord<StatEventKey, StatAggregate>>> outputRecordsMap =
                super.readAllProducerRecords(
                        topicDefinitionFactory.getAggregateTopicsMap().values(),
                        testDriver);

        assertThat(outputRecordsMap.keySet())
                .contains(expectedOutputTopics);

        outputRecordsMap.forEach((topicDef, producerRecord) -> {
            // only expecting output in one topic
                assertThat(outputRecordsMap.get(topicDef))
                        .hasSize(expectedCountInDesiredTopic);
        });
        return outputRecordsMap;
    }

    private void sendStatistics(final TopologyTestDriver testDriver,
                                final ConsumerRecordFactory<String, String> consumerRecordFactory,
                                final String uuid,
                                final Statistics statistics) {
        String xml = statisticsMarshaller.marshallToXml(statistics);

        super.sendMessage(testDriver, consumerRecordFactory, uuid, xml);
    }

    private StreamProcessor buildStreamProcessor(StatisticType statisticType) {

        StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory = new StatisticsFlatMappingStreamFactory(
                mockStatisticConfigurationService,
                topicDefinitionFactory,
                mockStroomPropertyService,
                new StatisticsMarshaller()
        );

        // TODO create a mock of this that does a noddy flat mapping as we already have tests for these impls
        StatisticFlatMapper statisticFlatMapper = new MockStatisticFlatMapper(mockStatisticConfigurationService);

        return new StatisticsFlatMappingProcessor(
                mockStroomPropertyService,
                topicDefinitionFactory,
                statisticsFlatMappingStreamFactory,
                statisticType,
                statisticFlatMapper);
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

    private void dumpStatistics(Statistics statisticsObj) {
        if (LOGGER.isTraceEnabled()) {
            statisticsObj.getStatistic().forEach(statistic -> {
                String tagValues = statistic.getTags().getTag().stream()
                        .map(tagValue -> tagValue.getName() + "|" + tagValue.getValue())
                        .collect(Collectors.joining(","));
                LOGGER.trace("Stat: {} {} {} {}",
                        statistic.getTime(),
                        tagValues,
                        statistic.getValue(),
                        statistic.getIdentifiers());
            });
        }
    }

    private static class MockStatisticFlatMapper implements StatisticFlatMapper{

        private final StatisticConfigurationService statisticConfigurationService;

        private MockStatisticFlatMapper(final StatisticConfigurationService statisticConfigurationService) {
            this.statisticConfigurationService = statisticConfigurationService;
        }

        @Override
        public Iterable<KeyValue<StatEventKey, StatAggregate>> flatMap(
                final String statUuid,
                final StatisticWrapper statisticWrapper) {

            StatisticConfiguration statisticConfiguration =statisticConfigurationService.fetchStatisticConfigurationByUuid(statUuid).get();

            statisticConfiguration.getPrecision();

            LOGGER.info("FlatMapping event to {} aggregates of type {} and interval {}",
                    ROLLUPS_PER_STAT, statisticConfiguration.getStatisticType(), statisticConfiguration.getPrecision());

            // One stat event flat maps to two rollups
            return IntStream.rangeClosed(0, ROLLUPS_PER_STAT - 1)
                    .mapToObj(i -> {
                        StatEventKey statEventKey = new StatEventKey(
                                UID.from(new byte[]{0,0,0,0}),
                                RollUpBitMask.fromShort((short)i),
                                 statisticConfiguration.getPrecision(),
                                Instant.now().toEpochMilli(),
                                Collections.emptyList());
                        StatAggregate statAggregate;
                        if (statisticConfiguration.getStatisticType().equals(StatisticType.COUNT)) {
                            statAggregate = new CountAggregate(
                                    Collections.singletonList(new MultiPartIdentifier(123L, 456L)),
                                    10,
                                    1000L
                            );
                        } else {
                            statAggregate = new ValueAggregate(
                                    Collections.singletonList(new MultiPartIdentifier(123L, 456L)),
                                    10,
                                    1000
                            );
                        }
                        return new KeyValue<>(statEventKey, statAggregate);
                    })
                    .collect(Collectors.toList());
        }
    }

}