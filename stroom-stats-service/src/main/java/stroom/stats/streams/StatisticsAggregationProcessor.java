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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import stroom.stats.StatisticsProcessor;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.hbase.uid.UID;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatEventKeySerde;
import stroom.stats.streams.topics.TopicDefinition;
import stroom.stats.streams.topics.TopicDefinitionFactory;
import stroom.stats.util.HasRunState;
import stroom.stats.util.logging.LambdaLogger;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * The following shows how the aggregation processing works for a single stat type
 * e.g. COUNT.  Events come in on one topic per aggregationInterval. Each aggregationInterval topic is
 * consumed and the events are aggregated together by StatEventKey (which all have their
 * time truncated to the aggregationInterval of the topic they came from.
 * <p>
 * Periodic flushes of the aggregated events are then forked to
 * the stat service for persistence and to the next biggest aggregationInterval topic for another
 * iteration. This waterfall approach imposes increasing latency as the intervals get bigger
 * but this should be fine as a query on the current DAY bucket will yield partial results as
 * the day is not yet over.
 * <p>
 * -------> consumer/producer SEC  -------->    statisticsService.putAggregatedEvents
 *   ________________________|
 *   V
 * -------> consumer/producer MIN  -------->    statisticsService.putAggregatedEvents
 *   ________________________|
 *   V
 * -------> consumer/producer HOUR -------->    statisticsService.putAggregatedEvents
 *   ________________________|
 *   V
 * -------> consumer/producer DAY  -------->    statisticsService.putAggregatedEvents
 *   ________________________|
 *   V
 * -------> consumer/producer FOREVER  ---->    statisticsService.putAggregatedEvents
 * <p>
 * If the system goes down unexpectedly then events that have been read off a topic but not yet committed
 * may be re-processed to some extent depending on when the shutdown happened, e.g duplicate events may go to
 * the next topic and/or to the stat service. The size of the StatAggregator is a trade off between in memory aggregation
 * benefits and the risk of more duplicate data in the stat store
 */
public class StatisticsAggregationProcessor implements StatisticsProcessor, TopicConsumer {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsAggregationProcessor.class);

    public static final String PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX = "stroom.stats.aggregation.processorAppIdPrefix";
    public static final String PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE = "stroom.stats.aggregation.minBatchSize";
    public static final String PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS = "stroom.stats.aggregation.maxFlushIntervalMs";
    public static final String PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS = "stroom.stats.aggregation.pollTimeoutMs";

    public static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECS = 120;

    private static final long PROPERTY_CHECK_INTERVAL_MS = 30_000;


    private final StatisticsService statisticsService;
    private final StroomPropertyService stroomPropertyService;
    private final StatisticType statisticType;
    private final EventStoreTimeIntervalEnum aggregationInterval;
    private final ConsumerFactory consumerFactory;
    private final int instanceId;
    private final AtomicReference<String> consumerThreadName = new AtomicReference<>();

    private volatile RunState runState = RunState.STOPPED;

    //used for thread synchronization
    private final Object startStopMonitor = new Object();

    private final ExecutorService executorService;
    private final Producer<StatEventKey, StatAggregate> kafkaProducer;
    private final TopicDefinition<StatEventKey, StatAggregate> inputTopic;
    private final String groupId;
    private final Optional<EventStoreTimeIntervalEnum> optNextInterval;
    private final Optional<TopicDefinition<StatEventKey, StatAggregate>> optNextIntervalTopic;

    private StatAggregator statAggregator;
    //    private Future<?> consumerFuture;
    private CompletableFuture<Void> consumerFuture;

    private Serde<StatEventKey> statKeySerde;
    private Serde<StatAggregate> statAggregateSerde;

    //variables to hold state for the health check
//    private Queue<Integer> assignedPartitions = new ConcurrentLinkedQueue<>();
    private Map<Integer, Long> latestPartitionOffsets = new ConcurrentHashMap<>();
    private final LongAdder msgCounter = new LongAdder();

    private long pollTimeOutLastFetchMs = 0;
    private long minBatchSizeLastFetchMs = 0;
    private long maxFlushIntervalLastFetchMs = 0;

    private Duration pollTimeOutLastValue = Duration.ofSeconds(3);
    private int minBatchSizeLastValue = 100;
    private int maxFlushIntervalLastValue = 10_000;

    //The following instance/class vars are there for debugging use
    //    private Map<StatEventKey, StatAggregate> putEventsMap = new HashMap<>();
//    public static final AtomicLong minTimestamp = new AtomicLong(Long.MAX_VALUE);
//    public static final ConcurrentMap<Integer, List<ConsumerRecord<StatEventKey, StatAggregate>>> consumerRecords =
//            new ConcurrentHashMap<>();

    public StatisticsAggregationProcessor(final TopicDefinitionFactory topicDefinitionFactory,
                                          final StatisticsService statisticsService,
                                          final StroomPropertyService stroomPropertyService,
                                          final StatisticType statisticType,
                                          final EventStoreTimeIntervalEnum aggregationInterval,
                                          final Producer<StatEventKey, StatAggregate> kafkaProducer,
                                          final ExecutorService executorService,
                                          final ConsumerFactory consumerFactory,
                                          final int instanceId) {

        this.statisticsService = statisticsService;
        this.stroomPropertyService = stroomPropertyService;
        this.statisticType = statisticType;
        this.aggregationInterval = aggregationInterval;
        this.instanceId = instanceId;
        this.kafkaProducer = kafkaProducer;
        this.executorService = executorService;
        this.consumerFactory = consumerFactory;

        LOGGER.info("Building {} - {} aggregation processor, with instance id {}",
                statisticType, aggregationInterval, instanceId);

        statKeySerde = StatEventKeySerde.instance();
        statAggregateSerde = StatAggregateSerde.instance();

        inputTopic = topicDefinitionFactory.getAggregatesTopic(statisticType, aggregationInterval);

        groupId = stroomPropertyService.getPropertyOrThrow(PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX) +
                "-" + inputTopic;
        optNextInterval = EventStoreTimeIntervalEnum.getNextBiggest(aggregationInterval);

        optNextIntervalTopic = optNextInterval.map(newInterval ->
                topicDefinitionFactory.getAggregatesTopic(statisticType, newInterval));

        //start a processor for a stat type and aggregationInterval pair
        //This will improve aggregation as it will only handle data for the same stat types and aggregationInterval sizes
    }

    private Consumer<StatEventKey, StatAggregate> buildConsumer() {
        try{
            LOGGER.debug("Starting aggregation consumer [{}]", instanceId);

            final Consumer<StatEventKey, StatAggregate> consumer = consumerFactory.createConsumer(
                    groupId, statKeySerde, statAggregateSerde);

            final StatisticsAggregationRebalanceListener rebalanceListener = new StatisticsAggregationRebalanceListener(
                    this,
                    consumer);

            consumer.subscribe(Collections.singletonList(inputTopic.getName()), rebalanceListener);

            //Update our collection of partitions for later health check use
//            assignedPartitions = kafkaConsumer.partitionsFor(inputTopic).stream()
//                    .map(PartitionInfo::partition)
//                    .collect(Collectors.toList());
            setAssignedPartitions(consumer.assignment());
            return consumer;
        } catch (Exception e) {
            LOGGER.error(String.format("Error building consumer for topic %s on processor %s", inputTopic, this), e);
            throw e;
        }
    }

    /**
     * Drain the aggregator and pass all aggregated events to the stat service to persist to the event store
     *
     * @param statisticType  The type of events being processed
     * @param statAggregator
     * @return The list of aggregates events drained from the aggregator and sent to the event store
     */
    private Map<StatEventKey, StatAggregate> flushToStatStore(final StatisticType statisticType,
                                                              final StatAggregator statAggregator) {

        Map<StatEventKey, StatAggregate> aggregatedEvents = statAggregator.getAggregates();

        //as our logger uses a lambda we need to assign this and make it final which is not ideal if
        //we are not in debug mode
        final Instant startTime = Instant.now();

        statisticsService.putAggregatedEvents(statisticType, statAggregator.getAggregationInterval(), aggregatedEvents);

        LOGGER.debug(() -> String.format("Flushed %s %s/%s events (from %s input events %.2f %%) to the StatisticsService in %sms",
                aggregatedEvents.size(), statisticType, statAggregator.getAggregationInterval(),
                statAggregator.getInputCount(), statAggregator.getAggregationPercentage(),
                Duration.between(startTime, Instant.now()).toMillis()));

        return aggregatedEvents;
    }

    private void flushToTopic(final StatAggregator statAggregator,
                              final String topic,
                              final EventStoreTimeIntervalEnum newInterval,
                              final Producer<StatEventKey, StatAggregate> producer) {

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

        consumerFuture = CompletableFuture.runAsync(this::consumerRunnable, executorService)
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error happened inside consumerRunnable on processor {}, {}", this, throwable.getMessage(), throwable);
                        runState = RunState.STOPPED;
                        throw new RuntimeException(throwable);
                    } else {
                        LOGGER.info("consumerRunnable finished cleanly for processor {}", this);
                    }
                });
    }

    private void consumerRunnable() {
        LOGGER.info("Starting consumer/producer for {}, {}, {} -> {}",
                statisticType, aggregationInterval, inputTopic,
                optNextIntervalTopic
                        .map(TopicDefinition::getName)
                        .orElse("None"));

        final Consumer<StatEventKey, StatAggregate> kafkaConsumer = buildConsumer();

        consumerThreadName.set(Thread.currentThread().getName());

        int unCommittedRecCount = 0;

        try {
            //loop forever unless the thread that is processing is stopped/interrupted from outside
            while (runState.equals(RunState.RUNNING) && !Thread.currentThread().isInterrupted()) {
                try {
                    final ConsumerRecords<StatEventKey, StatAggregate> records = kafkaConsumer.poll(getPollTimeoutMs());

                    int recCount = records.count();
                    // TODO should hook this in as a DropWiz metric
                    // Used in the healthcheck to give an indication of how much work this processor is doing
                    msgCounter.add(recCount);

                    unCommittedRecCount += recCount;
                    LOGGER.ifDebugIsEnabled(() -> {
                        debugRecords(records, recCount);
                    });

                    if (!records.isEmpty()) {
                        initStatAggregator();

                        for (ConsumerRecord<StatEventKey, StatAggregate> rec : records) {
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
                            //record the latest consumed offsets for each partition
                            latestPartitionOffsets.put(rec.partition(), rec.offset());
                        }
//                        LOGGER.debug("putEventsMap key count: {}", putEventsMap.size());
                    }

                    // Only checking if a flush is needed on each poll rather than on each msg
                    boolean flushHappened = flushAggregatorIfReady();
                    if (flushHappened && unCommittedRecCount > 0) {
                        kafkaConsumer.commitSync();
                        unCommittedRecCount = 0;
                    }
                } catch (Exception e) {
                    runState = RunState.STOPPED;
                    throw new RuntimeException(String.format("Error while polling with stat type %s on processor %s",
                            statisticType, this), e);
                }
            }
        } finally {
            //clean up as we are shutting down this processor
            LOGGER.debug("Breaking out of consumer loop, runState {}, interrupted state {} on processor {}",
                    runState, Thread.currentThread().isInterrupted(), this);
            cleanUp(kafkaConsumer, unCommittedRecCount);
        }
    }

    private void debugRecords(final ConsumerRecords<StatEventKey, StatAggregate> records, final int recCount) {
        final ConcurrentMap<UID, AtomicInteger> statNameMap = new ConcurrentHashMap<>();

        records.forEach(rec ->
                statNameMap.computeIfAbsent(
                        rec.key().getStatUuid(),
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

            final String minTimestampStr = Instant.ofEpochMilli(minTimestamp).toString();

//                            StatisticsAggregationProcessor.minTimestamp.getAndUpdate(currVal ->
//                                    Math.min(currVal, minTimestamp));

            //Capture all records received
//                            records.forEach(rec ->
//                                    consumerRecords.computeIfAbsent(rec.partition(), k -> new ArrayList<>()).add(rec));

            LOGGER.debug("Received {} records consisting of {}, on partitions {}, topic {}, min timestamp {}, processor {}",
                    recCount, breakdown, distinctPartitions, inputTopic, minTimestampStr, this);
        }
    }

    private void initStatAggregator() {
        if (statAggregator == null) {
            statAggregator = new StatAggregator(
                    getMinBatchSize(),
                    aggregationInterval,
                    getFlushIntervalMs());
        }
    }

    private void cleanUp(Consumer<StatEventKey, StatAggregate> kafkaConsumer, int unCommittedRecCount) {

        // force a flush of anything in the aggregator
        if (statAggregator != null) {
            LOGGER.debug("Forcing a flush of aggregator {} on processor {}", statAggregator, this);
            flushAggregator();
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
            return flushAggregator();
        } else {
            return false;
        }
    }

    private boolean flushAggregator() {
        // flush all the aggregated stats down to the StatStore and onto the next biggest aggregationInterval topic
        // (if there is one) for coarser aggregation
        if (statAggregator != null) {
            if (!statAggregator.isEmpty()) {
                LOGGER.trace("Flushing aggregator {}", statAggregator);

                flushToStatStore(statisticType, statAggregator);

                optNextInterval.ifPresent(nextInterval ->
                        flushToTopic(
                                statAggregator,
                                optNextIntervalTopic.get().getName(),
                                nextInterval,
                                kafkaProducer));
                //null the reference ready for new aggregates
                statAggregator = null;

                return true;
            }
        }
        return false;
    }

    void flush(final Consumer<StatEventKey, StatAggregate> kafkaConsumer) {
        flushAggregator();
        kafkaConsumer.commitSync();
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

    synchronized void setAssignedPartitions(@Nonnull Collection<TopicPartition> assignedPartitions) {
//        this.assignedPartitions.clear();
//        this.assignedPartitions.addAll(Preconditions.checkNotNull(assignedPartitions).stream()
//                .map(TopicPartition::partition)
//                .collect(Collectors.toList()));

        this.latestPartitionOffsets.clear();
        Preconditions.checkNotNull(assignedPartitions).stream()
                .map(TopicPartition::partition)
                .forEach(partition -> latestPartitionOffsets.put(partition, -1L));
    }

    @Override
    public HasRunState.RunState getRunState() {
        return runState;
    }

    @Override
    public String getName() {
        return groupId + "-" + instanceId;
    }

    @Override
    public HealthCheck.Result getHealth() {
        switch (runState) {
            case RUNNING:
                return HealthCheck.Result.builder()
                        .healthy()
                        .withMessage(runState.toString())
                        .withDetail("status", produceHealthCheckSummary())
                        .build();
            default:
                return HealthCheck.Result.builder()
                        .unhealthy()
                        .withMessage(runState.toString())
                        .withDetail("status", produceHealthCheckSummary())
                        .build();
        }
    }

    public Map<String, String> produceHealthCheckSummary() {

        //TODO accessing the variables in statAggregator is not safe as we are outside the thread that is mutating
        //the aggregator, may be sufficient for a health check peek.
        Map<String, String> statusMap = new TreeMap<>();

        statusMap.put("runState", runState.name());
        statusMap.put("inputTopic", inputTopic.getName());
        statusMap.put("groupId", groupId);
        statusMap.put("instanceId", Integer.toString(instanceId));
        statusMap.put("bufferInputCount",
                statAggregator == null
                        ? "-"
                        : String.format("%,d", statAggregator.getInputCount()));
        statusMap.put("size",
                statAggregator == null
                        ? "-"
                        : String.format("%,d", statAggregator.size()));
        statusMap.put("aggregation-compression-savings %",
                statAggregator == null
                        ? "-"
                        : String.format("%.2f", statAggregator.getAggregationPercentage()));
        statusMap.put("expiryTime",
                statAggregator == null
                        ? "-"
                        : statAggregator.getExpiryTime().toString());
        statusMap.put("partitionCount",
                statAggregator == null
                        ? "-"
                        : Integer.toString(latestPartitionOffsets.size()));
        statusMap.put("messageCounter", String.format("%,d", msgCounter.sum()));

        String latestPartitionOffsetsString = latestPartitionOffsets.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> entry.getKey() + ":" + (entry.getValue() == -1L ? "-" : entry.getValue()))
                .collect(Collectors.joining(", "));
        statusMap.put("latestConsumedPartitionOffsets", latestPartitionOffsetsString);

        return statusMap;
    }

    public int getInstanceId() {
        return instanceId;
    }

    private Duration getPollTimeoutMs() {
        // This method is called a lot so this an optimisation of an already cached value
        if (System.currentTimeMillis() > (pollTimeOutLastFetchMs + PROPERTY_CHECK_INTERVAL_MS)) {
            pollTimeOutLastFetchMs = System.currentTimeMillis();
            pollTimeOutLastValue = Duration.ofMillis(stroomPropertyService.getIntProperty(
                    PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS, 100));
        }
        return pollTimeOutLastValue;
    }

    private int getMinBatchSize() {
        // This method is called a lot so this an optimisation of an already cached value
        if (System.currentTimeMillis() > (minBatchSizeLastFetchMs + PROPERTY_CHECK_INTERVAL_MS)) {
            minBatchSizeLastFetchMs = System.currentTimeMillis();
            minBatchSizeLastValue = stroomPropertyService.getIntProperty(
                    PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10_000);
        }
        return minBatchSizeLastValue;
    }

    private int getFlushIntervalMs() {
        // This method is called a lot so this an optimisation of an already cached value
        if (System.currentTimeMillis() > (maxFlushIntervalLastFetchMs + PROPERTY_CHECK_INTERVAL_MS)) {
            maxFlushIntervalLastFetchMs = System.currentTimeMillis();
            maxFlushIntervalLastValue = stroomPropertyService.getIntProperty(
                    PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 60_000);
        }
        return maxFlushIntervalLastValue;
    }

    @Override
    public String toString() {
        return "{" +
                " " + statisticType +
                ", " + aggregationInterval +
                ", " + groupId +
                ", " + instanceId +
                ", " + runState +
                '}';
    }
}
