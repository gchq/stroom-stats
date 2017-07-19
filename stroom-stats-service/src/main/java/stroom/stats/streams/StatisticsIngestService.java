package stroom.stats.streams;

import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.StatisticsAggregationService;
import stroom.stats.util.HasRunState;
import stroom.stats.util.Startable;
import stroom.stats.util.Stoppable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.atomic.AtomicReference;

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
public class StatisticsIngestService implements Startable, Stoppable, HasRunState, Managed {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsIngestService.class);

    public static final String PROP_KEY_PREFIX_KAFKA = "stroom.stats.streams.kafka.";
    public static final String PROP_KEY_PREFIX_STATS_STREAMS = "stroom.stats.streams.";
    //properties mapped ot kafka internal configuration
    public static final String PROP_KEY_KAFKA_BOOTSTRAP_SERVERS = PROP_KEY_PREFIX_KAFKA + "bootstrapServers";
    public static final String PROP_KEY_KAFKA_COMMIT_INTERVAL_MS = PROP_KEY_PREFIX_KAFKA + "commit.interval.ms";
    public static final String PROP_KEY_KAFKA_STREAM_THREADS = PROP_KEY_PREFIX_KAFKA + "num.stream.threads";


    public static final String PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.statisticEventsPrefix";
    public static final String PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.badStatisticEventsPrefix";
    public static final String PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "stroom.stats.topics.statisticRollupPermsPrefix";

    private final StatisticsFlatMappingService statisticsFlatMappingService;
    private final StatisticsAggregationService statisticsAggregationService;

    @Override
    public HasRunState.RunState getRunState() {
        return null;
    }

    AtomicReference<RunState> runState = new AtomicReference<>(RunState.STOPPED);

    //used for thread synchronization
    private final Object startStopMonitor = new Object();

    @Inject
    public StatisticsIngestService(final StatisticsFlatMappingService statisticsFlatMappingService,
                                   final StatisticsAggregationService statisticsAggregationService) {

        this.statisticsFlatMappingService = statisticsFlatMappingService;
        this.statisticsAggregationService = statisticsAggregationService;
    }

    @SuppressWarnings("unused") //called by DropWizard's lifecycle manager
    @Override
    public void start() {


        //TODO may want to break this up to allow individual processor services or even
        //individual processor instances to be started/stopped.

        synchronized (startStopMonitor) {
            if (runState.compareAndSet(RunState.STOPPED, RunState.STARTING)) {
                LOGGER.info("Starting all processing");
                //start the aggregation processor first so it is ready to receive
                statisticsAggregationService.start();

                //start the stream processing
                statisticsFlatMappingService.start();

                changeRunState(RunState.STARTING, RunState.RUNNING);

            } else {
                LOGGER.warn("Unable to start processing as current state is {}", runState);
            }
        }
    }

    @SuppressWarnings("unused") //called by DropWizard's lifecycle manager
    @Override
    public void stop() {

        synchronized (startStopMonitor) {
            if (runState.compareAndSet(RunState.RUNNING, RunState.STOPPING)) {
                LOGGER.info("Stopping all processing");
                //start the stream processing
                statisticsFlatMappingService.stop();

                //start the aggregation processor first so it is ready to receive
                statisticsAggregationService.stop();

                changeRunState(RunState.STOPPING, RunState.STOPPED);
            } else {
                LOGGER.warn("Unable to stop processing as current state is {}", runState);
            }
        }
    }

    private void changeRunState(RunState expectedCurrentState, RunState newState) {

        if (!runState.compareAndSet(expectedCurrentState, newState)) {
            throw new RuntimeException(String.format("Failed to change state to %s, current state is %s, expecting %s",
                    newState, runState.get(), expectedCurrentState));
        }

    }
}
