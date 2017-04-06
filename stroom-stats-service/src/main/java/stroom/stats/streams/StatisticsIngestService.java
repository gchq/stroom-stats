package stroom.stats.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.StatisticsAggregationService;
import stroom.stats.api.StatisticType;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
public class StatisticsIngestService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsIngestService.class);

    public static final String PROP_KEY_PREFIX_KAFKA = "stroom.stats.streams.kafka.";
    public static final String PROP_KEY_PREFIX_STATS_STREAMS = "stroom.stats.streams.";
    //properties mapped ot kafka internal configuration
    public static final String PROP_KEY_KAFKA_BOOTSTRAP_SERVERS = PROP_KEY_PREFIX_KAFKA + "bootstrapServers";
    public static final String PROP_KEY_KAFKA_COMMIT_INTERVAL_MS = PROP_KEY_PREFIX_KAFKA + "commit.interval.ms";
    public static final String PROP_KEY_KAFKA_STREAM_THREADS = PROP_KEY_PREFIX_KAFKA + "num.stream.threads";

    public static final String PROP_KEY_FLAT_MAP_PROCESSOR_APP_ID_PREFIX = PROP_KEY_PREFIX_STATS_STREAMS + "flatMapProcessorAppIdPrefix";

    public static final String PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.statisticEventsPrefix";
    public static final String PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.badStatisticEventsPrefix";
    public static final String PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "stroom.stats.topics.statisticRollupPermsPrefix";


    private final StroomPropertyService stroomPropertyService;
    private final StatisticsFlatMappingService statisticsFlatMappingService;
    private final StatisticsAggregationService statisticsAggregationService;
    private final StatisticsAggregationProcessor statisticsAggregationProcessor;

    @Inject
    public StatisticsIngestService(final StroomPropertyService stroomPropertyService,
                                   final StatisticsFlatMappingService statisticsFlatMappingService,
                                   final StatisticsAggregationService statisticsAggregationService,
                                   final StatisticsAggregationProcessor statisticsAggregationProcessor) {

        this.stroomPropertyService = stroomPropertyService;
        this.statisticsFlatMappingService = statisticsFlatMappingService;
        this.statisticsAggregationService = statisticsAggregationService;
        this.statisticsAggregationProcessor = statisticsAggregationProcessor;
    }

    @SuppressWarnings("unused") //only called by guice
    @Inject
    public void startProcessors() {

        statisticsFlatMappingService.start();

        statisticsAggregationService.start();
    }
}
