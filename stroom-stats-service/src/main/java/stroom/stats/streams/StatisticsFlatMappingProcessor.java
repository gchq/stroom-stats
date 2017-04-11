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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.StatisticsProcessor;
import stroom.stats.api.StatisticType;
import stroom.stats.mixins.Startable;
import stroom.stats.mixins.Stoppable;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.streams.mapping.AbstractStatisticFlatMapper;

import java.util.HashMap;
import java.util.Map;

public class StatisticsFlatMappingProcessor implements StatisticsProcessor, Startable, Stoppable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsFlatMappingProcessor.class);

    private final StroomPropertyService stroomPropertyService;
    private final StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory;
    private final StatisticType statisticType;
    private KafkaStreams kafkaStreams;
    private final String appId;
    private final String inputTopic;
    private final String badEventTopic;
    private final String permsTopicsPrefix;
    private final AbstractStatisticFlatMapper mapper;

    public StatisticsFlatMappingProcessor(final StroomPropertyService stroomPropertyService,
                                          final StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory,
                                          final StatisticType statisticType,
                                          final AbstractStatisticFlatMapper mapper) {

        this.stroomPropertyService = stroomPropertyService;
        this.statisticsFlatMappingStreamFactory = statisticsFlatMappingStreamFactory;
        this.statisticType = statisticType;
        this.mapper = mapper;

        appId = getName(StatisticsIngestService.PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX, statisticType);
        LOGGER.info("Building flat mapping processor {}", appId);

        inputTopic = getName(StatisticsIngestService.PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType);
        badEventTopic = getName(StatisticsIngestService.PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType);
        permsTopicsPrefix = getName(StatisticsIngestService.PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType);

    }

    private KafkaStreams configureStream(final StatisticType statisticType,
                                               final AbstractStatisticFlatMapper mapper) {

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        //TODO need to specify number of threads in the yml as it could be box specific
        int streamThreads = getStreamThreads();
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);

        StreamsConfig streamsConfig = buildStreamsConfig(appId, props);

        KafkaStreams flatMapProcessor = statisticsFlatMappingStreamFactory.buildStream(
                streamsConfig,
                inputTopic,
                badEventTopic,
                permsTopicsPrefix,
                mapper);

        flatMapProcessor.setUncaughtExceptionHandler(buildUncaughtExceptionHandler(appId, statisticType, mapper));

        return flatMapProcessor;
    }

    private StreamsConfig buildStreamsConfig(String appId, final Map<String, Object> additionalProps) {
        Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, getStreamsCommitIntervalMs());

        //TODO not clear if this is needed for not. Normal Kafka doesn't need it but streams may do
        //leaving it in seems to cause zookeeper connection warnings in the tests.  Tests seem to work ok
        //without it
//        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConfig.getQuorum());

        //serdes will be defined explicitly in code

        //Add any additional props, overwriting any from above
        props.putAll(additionalProps);

        props.forEach((key, value) ->
                LOGGER.info("Setting Kafka Streams property {} for appId {} to [{}]", key, appId, value.toString())
        );

        return new StreamsConfig(props);
    }

    private Thread.UncaughtExceptionHandler buildUncaughtExceptionHandler(final String appId,
                                                                          final StatisticType statisticType,
                                                                          final AbstractStatisticFlatMapper abstractStatisticFlatMapper) {
        return (t, e) ->
                LOGGER.error("Uncaught exception in stream processor with appId {} type {} and mapper {} in thread {}",
                        appId,
                        statisticType,
                        abstractStatisticFlatMapper.getClass().getSimpleName(),
                        t.getName(),
                        e);
    }

    private String getName(final String propKey, final StatisticType statisticType) {
        String prefix = stroomPropertyService.getPropertyOrThrow(propKey);
        return TopicNameFactory.getStatisticTypedName(prefix, statisticType);
    }

    private int getStreamThreads() {
        return stroomPropertyService.getIntProperty(StatisticsIngestService.PROP_KEY_KAFKA_STREAM_THREADS, 1);
    }

    private long getStreamsCommitIntervalMs() {
        return stroomPropertyService.getLongProperty(StatisticsIngestService.PROP_KEY_KAFKA_COMMIT_INTERVAL_MS, 30_000);
    }

    private String getKafkaBootstrapServers() {
        return stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS);
    }

    @Override
    public void stop() {

        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        LOGGER.info("Stopped processor {} for input topic {}", appId, inputTopic);

    }

    @Override
    public void start() {

        kafkaStreams = configureStream(statisticType, mapper);
        kafkaStreams.start();
        LOGGER.info("Started processor {} for input topic {} with {} stream threads", appId, inputTopic, getStreamThreads());

    }
}
