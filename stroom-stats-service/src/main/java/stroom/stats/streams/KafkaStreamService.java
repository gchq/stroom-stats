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
 */

package stroom.stats.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.config.Config;
import stroom.stats.config.ZookeeperConfig;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.streams.mapping.AbstractStatisticMapper;
import stroom.stats.streams.mapping.CountStatToAggregateMapper;
import stroom.stats.streams.mapping.ValueStatToAggregateMapper;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 *This is the coordinator for the streams applications
 *it will define the config and initiate each application
 */
//TODO
/*
 singleton for now, probably need many instances of these stream processors
 partition by stat name, so single lookup of StatConfig per partition
 */
@Singleton
public class KafkaStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamService.class);


    public static final String PROP_KEY_PREFIX_KAFKA = "stroom.stats.streams.kafka.";
    public static final String PROP_KEY_PREFIX_STATS_STREAMS = "stroom.stats.streams.";
    //properties mapped ot kafka internal configuration
    public static final String PROP_KEY_KAFKA_BOOTSTRAP_SERVERS = PROP_KEY_PREFIX_KAFKA + "bootstrapServers";
    public static final String PROP_KEY_KAFKA_COMMIT_INTERVAL_MS = PROP_KEY_PREFIX_KAFKA + "commit.interval.ms";
    public static final String PROP_KEY_KAFKA_STREAM_THREADS = PROP_KEY_PREFIX_KAFKA + "num.stream.threads";

    public static final String PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX = PROP_KEY_PREFIX_STATS_STREAMS + "flatMapProcessorAppIdPrefix";
    public static final String PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX = PROP_KEY_PREFIX_STATS_STREAMS + "aggregationProcessorAppIdPrefix";

    public static final String PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.statisticEventsPrefix";
    public static final String PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.badStatisticEventsPrefix";
    public static final String PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "stroom.stats.topics.statisticRollupPermsPrefix";

    private static final EnumMap<StatisticType, Class<? extends StatAggregate>> statTypeToStatAggregateTypeMap = new EnumMap<>(StatisticType.class);
    static {
        statTypeToStatAggregateTypeMap.put(StatisticType.COUNT, CountAggregate.class);
        statTypeToStatAggregateTypeMap.put(StatisticType.VALUE, ValueAggregate.class);
    }

    private final StroomPropertyService stroomPropertyService;
    private final ZookeeperConfig zookeeperConfig;
    private final StatisticsFlatMappingProcessor statisticsFlatMappingProcessor;
    private final StatisticsAggregationProcessor statisticsAggregationProcessor;
    private final CountStatToAggregateMapper countStatToAggregateMapper;
    private final ValueStatToAggregateMapper valueStatToAggregateMapper;

    @Inject
    public KafkaStreamService(final StroomPropertyService stroomPropertyService,
                              final Config config,
                              final StatisticsFlatMappingProcessor statisticsFlatMappingProcessor,
                              final StatisticsAggregationProcessor statisticsAggregationProcessor,
                              final CountStatToAggregateMapper countStatToAggregateMapper,
                              final ValueStatToAggregateMapper valueStatToAggregateMapper) {
        this.stroomPropertyService = stroomPropertyService;
        this.zookeeperConfig = config.getZookeeperConfig();
        this.statisticsFlatMappingProcessor = statisticsFlatMappingProcessor;
        this.statisticsAggregationProcessor = statisticsAggregationProcessor;
        this.countStatToAggregateMapper = countStatToAggregateMapper;
        this.valueStatToAggregateMapper = valueStatToAggregateMapper;
    }



    @Inject
    public void start() {

        //TODO We probably want to separate the flatMapProcessors out into their own headless dropwiz service
        //so that we can scale the flat mapping independantly from the aggregation/putting to hbase.
        //The alternative is for everything to be in one main app but with each instance configured to have a set of
        //roles, e.g. just flatMapping or just aggregation/putting or both.
        //We may want to go even further by also splitting the aggregation/putting step out by stat type and
        //interval for total tuning control

        startFlatMapProcessing();

        //TODO commented out until it is in a working state
        //startAggregationProcessing();
    }

    private void startFlatMapProcessing() {
        LOGGER.info("Starting count flat map processor");
        KafkaStreams countFlatMapProcessor = startFlatMapProcessor(
                StatisticType.COUNT,
                countStatToAggregateMapper);

        LOGGER.info("Starting value flat map processor");
        KafkaStreams valueFlatMapProcessor = startFlatMapProcessor(
                StatisticType.VALUE,
                valueStatToAggregateMapper);
    }

    private void startAggregationProcessing() {

        //TODO either we have a single Count processor subscribing to all interval topics
        //or one Count processor per interval topic.  The latter means more tuning control.
        //Will use one for now (i.e. one per stat type)

        for (StatisticType statisticType : StatisticType.values()) {
            for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
                LOGGER.info("Starting aggregation processor for type {} and interval {}", statisticType, interval);
                startAggregationProcessor(statisticType, interval);
            }
        }
    }

    private StreamsConfig buildStreamsConfig(String appId, final Map<String, Object> additionalProps) {
        Map<String, Object> props = new HashMap<>();

        String kafkaBootstrapServers = stroomPropertyService.getPropertyOrThrow(PROP_KEY_KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        long commitIntervalMs = stroomPropertyService.getLongProperty(PROP_KEY_KAFKA_COMMIT_INTERVAL_MS, 30_000);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);

        //TODO not clear if this is needed for not. Normal Kafka doesn't need it but streams may do
        //leaving it in seems to cause zookeeper connection warnings in the tests.  Tests seem to work ok
        //without it
//        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConfig.getQuorum());

        //serdes will be defined explicitly in code

        //Add any additional props, overwriting any from above
        props.putAll(additionalProps);

        props.forEach((s, o) ->
            LOGGER.info("Setting Kafka Streams property {} for appId {} to [{}]", s, appId, o.toString())
        );

        return new StreamsConfig(props);
    }

    private Thread.UncaughtExceptionHandler buildUncaughtExceptionHandler(String appId) {
        return (t, e) -> LOGGER.error("Uncaught exception in stream processor with appId {} in thread {}", appId, t.getName(), e);
    }

    private KafkaStreams startFlatMapProcessor(final StatisticType statisticType,
                                               final AbstractStatisticMapper mapper) {

        String appId = getName(PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX, statisticType);

        LOGGER.info("Building processor {}", appId);

        String inputTopic = getName(PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType);
        String badEventTopic = getName(PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType);
        String permsTopicsPrefix = getName(PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType);

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        //TODO need to specify number of threads in the yml as it could be box specific
        int streamThreads = stroomPropertyService.getIntProperty(PROP_KEY_KAFKA_STREAM_THREADS, 1);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);

        StreamsConfig streamsConfig = buildStreamsConfig(appId, props);

        KafkaStreams flatMapProcessor = statisticsFlatMappingProcessor.buildStream(
                streamsConfig,
                inputTopic,
                badEventTopic,
                permsTopicsPrefix,
                mapper);

        flatMapProcessor.setUncaughtExceptionHandler(buildUncaughtExceptionHandler(appId));

        flatMapProcessor.start();

        LOGGER.info("Started processor {} for input topic {}", appId, inputTopic);

        return flatMapProcessor;
    }

    private KafkaStreams startAggregationProcessor(final StatisticType statisticType, final EventStoreTimeIntervalEnum interval) {

        String appId = getName(PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX, statisticType);

        LOGGER.info("Building processor {}", appId);

        String permsTopic = TopicNameFactory.getIntervalTopicName(PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval);

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        //TODO need to specify number of threads in the yml as it could be box specific
        //plus configure it on a per processor basis
        int streamThreads = stroomPropertyService.getIntProperty(PROP_KEY_KAFKA_STREAM_THREADS, 1);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);

        StreamsConfig streamsConfig = buildStreamsConfig(appId, props);

        KafkaStreams aggregationProcessor = statisticsAggregationProcessor.buildStream(
                streamsConfig,
                permsTopic,
                statTypeToStatAggregateTypeMap.get(statisticType));

        aggregationProcessor.setUncaughtExceptionHandler(buildUncaughtExceptionHandler(appId));

        aggregationProcessor.start();

        LOGGER.info("Started processor {} for input topic {}", appId, permsTopic);

        return aggregationProcessor;

    }

    private String getName(final String propKey, final StatisticType statisticType) {
        String prefix = stroomPropertyService.getPropertyOrThrow(propKey);
        return TopicNameFactory.getStatisticTypedName(prefix, statisticType);
    }


//        new KStreamBuilder().

        /*
        processor 1

        KStreamBuilder builder = new KStreamBuilder();
        builder
                .stream(new Serdes.StringSerde(), new Serdes.StringSerde(), "myStatsTopic")
                .map() //k=name/tagvalues/type/originalTime/smallestBucket v=eventObj
                .flatmap() //k=name/tagvalues/type/truncatedTime/smallestBucket v=aggregate, one for each rollup perm

        */

        /*
        processor 2 (one instance per time interval - S/M/H/D)
            .stream
            .selectKey() //k=name/tagvalues/type/truncatedTime/currentBucket v=aggregate, map the key only - truncate the time to the current bucket
            .aggregateByKey //aggregate within a tumbling window (period configured per bucket size, maybe)
            .through //add the current aggregates to a topic for load into hbase
            .filter() //currentBucket != largest bucket, to stop further processing
            .selectKey() //k=name/tagvalues/type/truncatedTime/nextBucket v=aggregate, map the key - move bucket to next biggest
            .to() //put on topic for this bucket size



         */

    //smallestBucket = Sec/Min/Hour/day/week/allTime
    //topics:
    //  statsFeed - incoming data from
    //  rawSecData - data for aggregation to Sec bucket
    //  rawMinData - data for aggregation to Min bucket
    //  rawHourData - data for aggregation to Hour bucket
    //  rawDayData - data for aggregation to Day bucket
    //  aggregatedData - data pre-aggregated to its bucket interval (all intervals in one topic)







}
