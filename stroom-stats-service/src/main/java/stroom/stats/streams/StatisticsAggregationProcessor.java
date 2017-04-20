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

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import stroom.stats.StatisticsProcessor;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.uid.UID;
import stroom.stats.mixins.HasRunState;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.stats.util.logging.LambdaLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * The following shows how the aggregation processing works for a single stat type
 * e.g. COUNT.  Events come in on one topic per aggregationInterval. Each aggregationInterval topic is
 * consumed and the events are aggregated together by StatKey (which all have their
 * time truncated to the aggregationInterval of the topic they came from.
 * <p>
 * Periodic flushes of the aggregated events are then forked to
 * the stat service for persistence and to the next biggest aggregationInterval topic for another
 * iteration. This waterfall approach imposes increasing latency as the intervals get bigger
 * but this should be fine as a query on the current DAY bucket will yield partial results as
 * the day is not yet over.
 * <p>
 * -------> consumer/producer SEC  -------->    statisticsService.putAggregatedEvents
 * __________________________|
 * V
 * -------> consumer/producer MIN  -------->    statisticsService.putAggregatedEvents
 * __________________________|
 * V
 * -------> consumer/producer HOUR -------->    statisticsService.putAggregatedEvents
 * __________________________|
 * V
 * -------> consumer/producer DAY  -------->    statisticsService.putAggregatedEvents
 * <p>
 * If the system goes down unexpectedly then events that have been read off a topic but not yet committed
 * may be re-processed to some extent depending on when the shutdown happened, e.g duplicate events may go to
 * the next topic and/or to the stat service. The size of the StatAggregator is a trade off between in memory aggregation
 * benefits and the risk of more duplicate data in the stat store
 */
public class StatisticsAggregationProcessor implements StatisticsProcessor {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsAggregationProcessor.class);

    public static final String PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX = "stroom.stats.aggregation.processorAppIdPrefix";
    public static final String PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE = "stroom.stats.aggregation.minBatchSize";
    public static final String PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS = "stroom.stats.aggregation.maxFlushIntervalMs";
    public static final String PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS = "stroom.stats.aggregation.pollTimeoutMs";
    public static final String PROP_KEY_AGGREGATOR_POLL_RECORDS = "stroom.stats.aggregation.pollRecords";

    public static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECS = 120;

    private final StatisticsService statisticsService;
    private final StroomPropertyService stroomPropertyService;
    private final StatisticType statisticType;
    private final EventStoreTimeIntervalEnum aggregationInterval;
    private final int instanceId;
    private final int maxEventIds;
    private final AtomicReference<String> consumerThreadName = new AtomicReference<>();

    private volatile RunState runState = RunState.STOPPED;

    //used for thread synchronization
    private final Object startStopMonitor = new Object();

    private final ExecutorService executorService;
    private final KafkaProducer<StatKey, StatAggregate> kafkaProducer;
    private final String inputTopic;
    private final String groupId;
    private final Optional<EventStoreTimeIntervalEnum> optNextInterval;
    private final Optional<String> optNextIntervalTopic;

    private StatAggregator statAggregator;
    //    private Future<?> consumerFuture;
    private CompletableFuture<Void> consumerFuture;

    private Serde<StatKey> statKeySerde;
    private Serde<StatAggregate> statAggregateSerde;

    //    private Map<StatKey, StatAggregate> putEventsMap = new HashMap<>();
    public static final LongAdder msgCounter = new LongAdder();
    public static final AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);
    public static final ConcurrentMap<Integer, List<ConsumerRecord<StatKey, StatAggregate>>> consumerRecords =
            new ConcurrentHashMap<>();

    public StatisticsAggregationProcessor(final StatisticsService statisticsService,
                                          final StroomPropertyService stroomPropertyService,
                                          final StatisticType statisticType,
                                          final EventStoreTimeIntervalEnum aggregationInterval,
                                          final KafkaProducer<StatKey, StatAggregate> kafkaProducer,
                                          final ExecutorService executorService,
                                          final int instanceId) {

        this.statisticsService = statisticsService;
        this.stroomPropertyService = stroomPropertyService;
        this.statisticType = statisticType;
        this.aggregationInterval = aggregationInterval;
        this.instanceId = instanceId;
        this.kafkaProducer = kafkaProducer;
        this.executorService = executorService;

        LOGGER.info("Building aggregation processor for type {}, aggregationInterval {}, and instance id {}",
                statisticType, aggregationInterval, instanceId);

        maxEventIds = getMaxEventIds();
        statKeySerde = StatKeySerde.instance();
        statAggregateSerde = StatAggregateSerde.instance();

        String topicPrefix = stroomPropertyService.getPropertyOrThrow(
                StatisticsIngestService.PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX);

        inputTopic = TopicNameFactory.getIntervalTopicName(topicPrefix, statisticType, aggregationInterval);
        groupId = stroomPropertyService.getPropertyOrThrow(PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX) +
                "-" + inputTopic;
        optNextInterval = EventStoreTimeIntervalHelper.getNextBiggest(aggregationInterval);
        optNextIntervalTopic = optNextInterval.map(newInterval ->
                TopicNameFactory.getIntervalTopicName(topicPrefix, statisticType, newInterval));

        int maxEventIds = getMaxEventIds();
        long minBatchSize = getMinBatchSize();

        //start a processor for a stat type and aggregationInterval pair
        //This will improve aggregation as it will only handle data for the same stat types and aggregationInterval sizes
    }

    private KafkaConsumer<StatKey, StatAggregate> buildConsumer() {

        try {
            KafkaConsumer<StatKey, StatAggregate> kafkaConsumer = new KafkaConsumer<>(
                    getConsumerProps(),
                    statKeySerde.deserializer(),
                    statAggregateSerde.deserializer());

            kafkaConsumer.subscribe(Collections.singletonList(inputTopic));

            return kafkaConsumer;
        } catch (Exception e) {
            LOGGER.error(String.format("Error building consumer for topic %s on processor %s", inputTopic, this), e);
            throw e;
        }
    }

    private Map<String, Object> getConsumerProps() {

        Map<String, Object> consumerProps = new HashMap<>();

        //ensure the session timeout is bigger than the flush timeout so our session doesn't expire
        //before we try to commit after a flush
        //Also ensure it isn't less than the default of 10s
        int sessionTimeoutMs = Math.max(10_000, (int) (getFlushIntervalMs() * 1.2));

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getPollRecords());
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
//        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        return consumerProps;
    }


    /**
     * Drain the aggregator and pass all aggregated events to the stat service to persist to the event store
     *
     * @param statisticType  The type of events being processed
     * @param statAggregator
     * @return The list of aggregates events drained from the aggregator and sent to the event store
     */
    private Map<StatKey, StatAggregate> flushToStatStore(final StatisticType statisticType,
                                                         final StatAggregator statAggregator) {

        Map<StatKey, StatAggregate> aggregatedEvents = statAggregator.getAggregates();

        //as our logger uses a lambda we need to assign this and make it final which is not ideal if
        //we are not in debug mode
        final Instant startTime = Instant.now();

        statisticsService.putAggregatedEvents(statisticType, statAggregator.getAggregationInterval(), aggregatedEvents);

        LOGGER.debug(() -> String.format("Flushed %s %s/%s events (from %s input events %.2f %%) to the StatisticsService in %sms",
                aggregatedEvents.size(), statisticType, statAggregator.getAggregationInterval(),
                statAggregator.getInputCount(), statAggregator.getReductionPercentage(),
                Duration.between(startTime, Instant.now()).toMillis()));

        return aggregatedEvents;
    }

    private void flushToTopic(final StatAggregator statAggregator,
                              final String topic,
                              final EventStoreTimeIntervalEnum newInterval,
                              final KafkaProducer<StatKey, StatAggregate> producer) {

        Preconditions.checkNotNull(statAggregator);
        Preconditions.checkNotNull(producer);

        //as our logger uses a lambda we need to assign this and make it final which is not ideal if
        //we are not in debug mode
        final Instant startTime = Instant.now();

        //Uplift the statkey to the new aggregationInterval and put it on the topic
        //We will not be trying to uplift the statKey if we are already at the highest aggregationInterval
        //so the RTE that cloneAndChangeInterval can throw should never happen
        statAggregator.getAggregates().entrySet().stream()
                .map(entry -> new ProducerRecord<>(
                        topic,
                        entry.getKey().cloneAndChangeInterval(newInterval),
                        entry.getValue()))
                .peek(producerRecord -> LOGGER.trace("Putting record {} on topic {}", producerRecord, topic))
                .forEach(producer::send);

        LOGGER.debug(() -> String.format("Flushed %s records from interval %s with new interval %s to topic %s in %sms",
                statAggregator.size(), statAggregator.getAggregationInterval(), newInterval, topic, Duration.between(startTime, Instant.now()).toMillis()));

        producer.flush();
    }

    private void startProcessor() {
        runState = RunState.RUNNING;

//        consumerFuture = executorService.submit(this::consumerRunnable);
        consumerFuture = CompletableFuture.runAsync(this::consumerRunnable, executorService)
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error happened inside consumerRunnable on processor {}, {}", this, throwable.getMessage(), throwable);
                        runState = RunState.STOPPED;
                        throw new RuntimeException(throwable);
                    } else {
                        LOGGER.info("consumerRunnable finished cleanly for processor", this);
                    }
                });
    }

    private void consumerRunnable() {
        LOGGER.info("Starting consumer/producer for {}, {}, {} -> {}",
                statisticType, aggregationInterval, inputTopic, optNextIntervalTopic.orElse("None"));

        KafkaConsumer<StatKey, StatAggregate> kafkaConsumer = buildConsumer();

        consumerThreadName.set(Thread.currentThread().getName());

        int unCommittedRecCount = 0;

        try {
            //loop forever unless the thread is processing is stopped from outside
            while (runState.equals(RunState.RUNNING) && !Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<StatKey, StatAggregate> records = kafkaConsumer.poll(getPollTimeoutMs());

                    int recCount = records.count();
                    msgCounter.add(recCount);
                    unCommittedRecCount += recCount;
                    LOGGER.ifDebugIsEnabled(() -> {
                        ConcurrentMap<UID, AtomicInteger> statNameMap = new ConcurrentHashMap<>();

                        records.forEach(rec ->
                                statNameMap.computeIfAbsent(
                                        rec.key().getStatName(),
                                        k -> new AtomicInteger(0)
                                ).incrementAndGet());

                        if (recCount > 0) {
                            String breakdown = statNameMap.entrySet().stream()
                                    .map(entry -> entry.getKey().toString() + "-" + entry.getValue().get())
                                    .collect(Collectors.joining(","));

                            String distinctPartitions = StreamSupport.stream(records.spliterator(), false)
                                    .map(rec -> String.valueOf(rec.partition()))
                                    .distinct()
                                    .collect(Collectors.joining(","));

                            long minTimestamp = StreamSupport.stream(records.spliterator(), false)
                                    .mapToLong(ConsumerRecord::timestamp)
                                    .min()
                                    .getAsLong();

                            String minTimestampStr = Instant.ofEpochMilli(minTimestamp).toString();

                            StatisticsAggregationProcessor.minTimestamp.getAndUpdate(currVal ->
                                    Math.min(currVal, minTimestamp));

                            //Capture all records received
                            records.forEach(rec ->
                                consumerRecords.computeIfAbsent(rec.partition(), k -> new ArrayList<>()).add(rec));

                            LOGGER.debug("Received {} records consisting of {}, on partitions {}, topic {}, min timestamp {}, processor {}",
                                    recCount, breakdown, distinctPartitions, inputTopic, minTimestampStr, this);
                        }
                    });

                    if (!records.isEmpty()) {
                        if (statAggregator == null) {
                            statAggregator = new StatAggregator(
                                    getMinBatchSize(),
                                    getMaxEventIds(),
                                    aggregationInterval,
                                    getFlushIntervalMs());
                        }

                        records.forEach(rec -> {

//                            LOGGER.ifDebugIsEnabled(() -> {
//
//                                putEventsMap.computeIfPresent(rec.key(), (k, v) -> {
//                                    if (rec.key().getRollupMask().equals(RollUpBitMask.ZERO_MASK) &&
//                                            aggregationInterval.equals(EventStoreTimeIntervalEnum.SECOND)) {
//
//                                        LOGGER.debug("Existing key {}", k.toString());
//                                        LOGGER.debug("New      key {}", rec.key().toString());
//                                        LOGGER.debug("Seen duplicate key");
//                                    }
//                                    return v.aggregate(rec.value(), 100);
//                                });
//                                putEventsMap.put(rec.key(), rec.value());
//                            });

                            statAggregator.add(rec.key(), rec.value());
                        });

//                        LOGGER.debug("putEventsMap key count: {}", putEventsMap.size());
                    }

                    boolean flushHappened = flushAggregatorIfReady();
                    if (flushHappened && unCommittedRecCount > 0) {
                        kafkaConsumer.commitSync();
                        unCommittedRecCount = 0;
                    }
                } catch (Exception e) {
                    runState = RunState.STOPPED;
                    throw new RuntimeException(String.format("Error while polling with stat type %s on processor %s", statisticType, this), e);
                }
            }
        } finally {
            //clean up as we are shutting down this processor
            LOGGER.debug("Breaking out of consumer loop, runState {}, interrupted state {} on processor {}",
                    runState, Thread.currentThread().isInterrupted(), this);
            cleanUp(kafkaConsumer, unCommittedRecCount);
        }
    }

    private void cleanUp(KafkaConsumer<StatKey, StatAggregate> kafkaConsumer, int unCommittedRecCount) {

        //force a flush of anything in the aggregator
        if (statAggregator != null) {
            LOGGER.debug("Forcing a flush of aggregator {} on processor {}", statAggregator, this);
            flushAggregator(statAggregator);
        }
        if (kafkaConsumer != null) {
            if (unCommittedRecCount > 0) {
                LOGGER.debug("Committing kafka offset on processor {}", this);
                kafkaConsumer.commitSync();
            }
            LOGGER.debug("Closing kafka consumer on processor {}", this);
            kafkaConsumer.close();
        }
    }

    private boolean flushAggregatorIfReady() {

        if (statAggregator != null && statAggregator.isReadyForFlush()) {
            if (statAggregator.isEmpty()) {
                //null the variable so we create a new one when we actually have records
                statAggregator = null;
                return false;
            } else {
                flushAggregator(statAggregator);
                //null the reference ready for new aggregates
                statAggregator = null;
                return true;
            }
        }
        return false;
    }

    private void flushAggregator(StatAggregator statAggregator) {
        //flush all the aggregated stats down to the StatStore and onto the next biggest aggregationInterval topic
        //(if there is one) for coarser aggregation
        if (statAggregator != null) {
            LOGGER.trace("Flushing aggregator {}", statAggregator);

            flushToStatStore(statisticType, statAggregator);

            optNextInterval.ifPresent(nextInterval ->
                    flushToTopic(statAggregator, optNextIntervalTopic.get(), nextInterval, kafkaProducer));
        }
    }


    @Override
    public void stop() {

        synchronized (startStopMonitor) {
            switch (runState) {
                case STOPPED:
                    LOGGER.info("Aggregation processor {} is already stopped", this);
                    break;
                case STOPPING:
                    LOGGER.info("Aggregation processor {} is already stopping", this);
                    break;
                case RUNNING:
                    doStop();
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected runState " + runState);
            }
        }
    }

    private void doStop() {
        LOGGER.info("Stopping Aggregation processor {} with timeout {}s", toString(), EXECUTOR_SHUTDOWN_TIMEOUT_SECS);
        //change the run state so the consumer thread will cleanly finish when it next
        //checks the variable
        runState = RunState.STOPPING;
        Instant start = Instant.now();

        try {
            //wait for it to cleanly stop having interrupted its thread, no result to check
            consumerFuture.get(EXECUTOR_SHUTDOWN_TIMEOUT_SECS, TimeUnit.SECONDS);
            LOGGER.info("{} terminated in {}s",
                    this.toString(), Duration.between(start, Instant.now()).getSeconds());
            runState = RunState.STOPPED;
        } catch (InterruptedException e) {
            LOGGER.error("Thread {} interrupted trying to shut down executor service",
                    Thread.currentThread().getName());
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Error in consumer thread for processor {}: {}", this.toString(), e.getMessage(), e);
        } catch (TimeoutException e) {
            LOGGER.error("Executor service could not be terminated in {}s",
                    Duration.between(start, Instant.now()).getSeconds());
        } catch (CancellationException e) {
        }
    }

    @Override
    public void start() {
        synchronized (startStopMonitor) {
            switch (runState) {
                case STOPPED:
                    LOGGER.info("Starting Aggregation processor {}", toString());
                    startProcessor();
                    break;
                case STOPPING:
                    throw new RuntimeException(String.format("Cannot start processor %s as it is currently stopping", toString()));
                case RUNNING:
                    LOGGER.info("Aggregation processor {} is already running", toString());
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected runState " + runState);
            }
        }
    }

    @Override
    public HasRunState.RunState getRunState() {
        return runState;
    }

    @Override
    public String getGroupId() {
        return groupId;
    }

    @Override
    public String getName() {
        return groupId + "-" + instanceId;
    }

    @Override
    public HealthCheck.Result check() {
        switch (runState) {
            case RUNNING:
                return HealthCheck.Result.healthy(produceHealthCheckSummary());
            default:
                return HealthCheck.Result.unhealthy(runState.toString());
        }
    }

    private String produceHealthCheckSummary() {

        //TODO accessing the variables in statAggregator is not safe as we are outside the thread that is mutating
        //the aggregator, may be sufficient for a health check peek.
        return String.format("%s - buffer input count: %s, size: %s, %% reduction: %.2f, expiredTime: %s",
                runState,
                (statAggregator == null ? "null" : statAggregator.getInputCount()),
                (statAggregator == null ? "null" : statAggregator.size()),
                (statAggregator == null ? 0 : statAggregator.getReductionPercentage()),
                (statAggregator == null ? "null" : statAggregator.getExpiredTime().toString()));
    }

    public int getInstanceId() {
        return instanceId;
    }

    private int getPollTimeoutMs() {
        return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS, 100);
    }

    private int getPollRecords() {
        return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_POLL_RECORDS, 1000);
    }

    private int getMinBatchSize() {
        return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10_000);
    }

    private int getMaxEventIds() {
        return stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
    }

    private int getFlushIntervalMs() {
        return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 60_000);
    }

    @Override
    public String toString() {
        return "StatisticsAggregationProcessor{" +
                " " + statisticType +
                ", " + aggregationInterval +
                ", " + groupId +
                ", " + instanceId +
                ", " + runState +
                '}';
    }

}
