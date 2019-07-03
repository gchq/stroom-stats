package stroom.stats.streams;

import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.schema.v4.StatisticsMarshaller;
import stroom.stats.streams.mapping.AbstractStatisticFlatMapper;
import stroom.stats.streams.mapping.CountStatToAggregateFlatMapper;
import stroom.stats.streams.mapping.ValueStatToAggregateFlatMapper;
import stroom.stats.streams.topics.TopicDefinition;
import stroom.stats.streams.topics.TopicDefinitionFactory;
import stroom.stats.test.StatisticsHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TestStatisticsFlatMappingProcessor extends AbstractStreamProcessorTest {

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

//    @Mock
//    StroomPropertyService mockStroomPropertyService;


    @Mock
    StatisticConfigurationService mockStatisticConfigurationService;

    MockUniqueIdCache mockUniqueIdCache = new MockUniqueIdCache();

    MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();

    TopicDefinitionFactory topicDefinitionFactory = new TopicDefinitionFactory(mockStroomPropertyService);

    @Test
    public void name() {

        StatisticType statisticType = StatisticType.COUNT;

        TopicDefinition inputTopic = topicDefinitionFactory.createStatisticEventsTopic(statisticType);

        StreamProcessor streamProcessor = buildStreamProcessor(statisticType);

        runProcessorTest(inputTopic, streamProcessor, (testDriver, consumerRecordFactory) -> {

        });

    }

    private StreamProcessor buildStreamProcessor(StatisticType statisticType) {

        StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory = new StatisticsFlatMappingStreamFactory(
                mockStatisticConfigurationService,
                topicDefinitionFactory,
                mockStroomPropertyService,
                new StatisticsMarshaller()
        );

        // TODO create a mock of this that does a noddy flat mapping as we already have tests for these impls
        AbstractStatisticFlatMapper statisticFlatMapper = statisticType.equals(StatisticType.COUNT)
                ? new CountStatToAggregateFlatMapper(mockUniqueIdCache, mockStroomPropertyService)
                : new ValueStatToAggregateFlatMapper(mockUniqueIdCache, mockStroomPropertyService);

        return new StatisticsFlatMappingProcessor(
                mockStroomPropertyService,
                topicDefinitionFactory,
                statisticsFlatMappingStreamFactory,
                statisticType,
                statisticFlatMapper);
    }

}