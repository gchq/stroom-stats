package stroom.stats.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.assertj.core.api.Assertions;
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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    public void twoGoodCountEvents() {

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
    public void twoGoodValueEvents() {

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
            List<ProducerRecord<StatEventKey, StatAggregate>> aggregateRecords = getAndAssertOutputAggregates(
                    testDriver, expectedAggregateRecordCount, 0, expectedOutputTopic);

            List<ProducerRecord<String, String>> badRecords = getAndAssertBadEvents(
                    testDriver, 0, 0, badStatEventsTopic);
        });
    }

    private List<ProducerRecord<String, String>> getAndAssertBadEvents(
            final TopologyTestDriver testDriver,
            final int expectedCountInDesiredTopic,
            final int expectedCountInOtherTopics,
            final TopicDefinition<String, String> expectedTopic) {

        final Map<TopicDefinition<String, String>, List<ProducerRecord<String, String>>> outputRecordsMap =
                super.readAllProducerRecords(
                        topicDefinitionFactory.getBadStatisticEventTopicsMap().values(),
                        testDriver);

        outputRecordsMap.forEach((topicDef, producerRecord) -> {
            // only expecting output in one topic
            if (topicDef.equals(expectedTopic)) {
                Assertions
                        .assertThat(outputRecordsMap.get(topicDef))
                        .hasSize(expectedCountInDesiredTopic);
            } else {
                Assertions
                        .assertThat(outputRecordsMap.get(topicDef))
                        .hasSize(expectedCountInOtherTopics);
            }
        });
        return outputRecordsMap.get(expectedTopic);
    }


    private List<ProducerRecord<StatEventKey, StatAggregate>> getAndAssertOutputAggregates(
            final TopologyTestDriver testDriver,
            final int expectedCountInDesiredTopic,
            final int expectedCountInOtherTopics,
            final TopicDefinition<StatEventKey, StatAggregate> expectedOutputTopic) {

        final Map<TopicDefinition<StatEventKey, StatAggregate>, List<ProducerRecord<StatEventKey, StatAggregate>>> outputRecordsMap =
                super.readAllProducerRecords(
                        topicDefinitionFactory.getAggregateTopicsMap().values(),
                        testDriver);

        outputRecordsMap.forEach((topicDef, producerRecord) -> {
            // only expecting output in one topic
            if (topicDef.equals(expectedOutputTopic)) {
                Assertions
                        .assertThat(outputRecordsMap.get(topicDef))
                        .hasSize(expectedCountInDesiredTopic);
            } else {
                Assertions
                        .assertThat(outputRecordsMap.get(topicDef))
                        .hasSize(expectedCountInOtherTopics);
            }
        });
        return outputRecordsMap.get(expectedOutputTopic);
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