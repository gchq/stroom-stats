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
import stroom.stats.mixins.Startable;
import stroom.stats.mixins.Stoppable;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The following shows how the aggregation processing works for a single stat type
 * e.g. COUNT.  Events come in on one topic per interval. Each interval topic is
 * consumed and the events are aggregated together by StatKey (which all have their
 * time truncated to the interval of the topic they came from.
 *
 * Periodic flushes of the aggregated events are then forked to
 * the stat service for persistence and to the next biggest interval topic for another
 * iteration. This waterfall approach imposes increasing latency as the intervals get bigger
 * but this should be fine as a query on the current DAY bucket will yield partial results as
 * the day is not yet over.
 * <p>
 * -------> consumer/producer SEC  -------->    statisticsService.putAggregatedEvents
 *       __________________________|
 *      V
 * -------> consumer/producer MIN  -------->    statisticsService.putAggregatedEvents
 *       __________________________|
 *      V
 * -------> consumer/producer HOUR -------->    statisticsService.putAggregatedEvents
 *       __________________________|
 *      V
 * -------> consumer/producer DAY  -------->    statisticsService.putAggregatedEvents
 * <p>
 * If the system goes down unexpectedly then events that have been read off a topic but not yet committed
 * may be re-processed to some extent depending on when the shutdown happened, e.g duplicate events may go to
 * the next topic and/or to the stat service. The size of the StatAggregator is a trade off between in memory aggregation
 * benefits and the risk of more duplicate data in the stat store
 */
class StatisticsAggregationProcessor implements StatisticsProcessor, Startable, Stoppable {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsAggregationProcessor.class);

    public static final String PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE = "stroom.stats.aggregation.minBatchSize";
    public static final String PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS = "stroom.stats.aggregation.maxFlushIntervalMs";
    public static final String PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS = "stroom.stats.aggregation.pollTimeoutMs";

    private final StatisticsService statisticsService;
    private final StroomPropertyService stroomPropertyService;
    private final int maxEventIds;

    //TODO get the StatAggService to create these and hold the instances for subsequent management
    @Inject
    public StatisticsAggregationProcessor(final StatisticsService statisticsService,
                                          final StroomPropertyService stroomPropertyService) {

        this.statisticsService = statisticsService;
        this.stroomPropertyService = stroomPropertyService;

        maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
    }

    public void startProcessor(final Map<String, Object> consumerProps,
                               final Map<String, Object> producerProps,
                               final String inputTopic,
                               final Optional<EventStoreTimeIntervalEnum> nextInterval,
                               final Optional<String> nextIntervalTopic,
                               final StatisticType statisticType,
                               final EventStoreTimeIntervalEnum aggregationInterval) {

        //TODO split this method into configuring the consumer and initiating the polling
        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        int maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
        int maxFlushIntervalMs = stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 60_000);
        long minBatchSize = stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10_000);

        Map<String, Object> customConsumerProps = new HashMap<>();
        customConsumerProps.putAll(consumerProps);
        customConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-" + inputTopic);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<StatKey, StatAggregate> kafkaConsumer = new KafkaConsumer<>(customConsumerProps,
                    statKeySerde.deserializer(),
                    statAggregateSerde.deserializer());

            //TODO need to share this between all interval/statTypes as it is thread-safe and config should be consistent
            KafkaProducer<StatKey, StatAggregate> kafkaProducer = null;
            if (nextIntervalTopic.isPresent()) {
                kafkaProducer = new KafkaProducer<>(producerProps,
                        statKeySerde.serializer(),
                        statAggregateSerde.serializer());
            }

            LOGGER.info("Starting consumer/producer for {}, {}, {} -> {}",
                    statisticType, aggregationInterval, inputTopic, nextIntervalTopic.orElse("None"));

            kafkaConsumer.subscribe(Collections.singletonList(inputTopic));

            //Because the batch size threshold is a min the number of items will likely go over that so size
            //the aggregator to be 20% bigger
            StatAggregator statAggregator = new StatAggregator((int) (minBatchSize * 1.2), maxEventIds, aggregationInterval);

            final Instant lastCommitTime = Instant.now();

            try {
                //TODO test for a flag set by the close methods to allow the polling to stop cleanly
                while (true) {
                    try {
                        ConsumerRecords<StatKey, StatAggregate> records = kafkaConsumer.poll(getPollTimeout());

                        LOGGER.ifTraceIsEnabled(() -> {
                            int recCount = records.count();
                            if (recCount > 0) {
                                LOGGER.trace("Received {} records from topic {}", records.count(), inputTopic);
                            }
                        });

                        for (ConsumerRecord<StatKey, StatAggregate> record : records) {
                            statAggregator.add(record.key(), record.value());
                        }

                        //flush if the aggregator is too big or it has been too long since the last flush
                        if (statAggregator.size() > 0 &&
                                (statAggregator.size() >= minBatchSize ||
                                        Duration.between(lastCommitTime, Instant.now()).toMillis() > maxFlushIntervalMs)) {

                            //flush all the aggregated stats down to the StatStore and onto the next biggest interval topic
                            //(if there is one) for coarser aggregation
                            Map<StatKey, StatAggregate> aggregatedEvents = flushToStatStore(statisticType, statAggregator);

                            if (nextInterval.isPresent()) {
                                flushToTopic(aggregatedEvents, nextIntervalTopic.get(), nextInterval.get(), kafkaProducer);
                            }
                            kafkaConsumer.commitSync();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error while polling with stat type {}", statisticType, e);
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });
    }


    /**
     * Drain the aggregator and pass all aggregated events to the stat service to persist to the event store
     *
     * @param statisticType  The type of events being processed
     * @param statAggregator
     * @return The list of aggregates events drained from the aggregator and sent to the event store
     */
    private Map<StatKey, StatAggregate> flushToStatStore(final StatisticType statisticType, final StatAggregator statAggregator) {
        Map<StatKey, StatAggregate> aggregatedEvents = statAggregator.drain();
        LOGGER.trace(() -> String.format("Flushing %s events of type %s, interval %s to the StatisticsService",
                aggregatedEvents.size(), statisticType, statAggregator.getAggregationInterval()));

        statisticsService.putAggregatedEvents(statisticType, statAggregator.getAggregationInterval(), aggregatedEvents);
        return aggregatedEvents;
    }

    private void flushToTopic(final Map<StatKey, StatAggregate> aggregatedEvents,
                              final String topic,
                              final EventStoreTimeIntervalEnum newInterval,
                              final KafkaProducer<StatKey, StatAggregate> producer) {

        Preconditions.checkNotNull(aggregatedEvents);
        Preconditions.checkNotNull(producer);

        LOGGER.trace(() -> String.format("Flushing %s records with new interval %s to topic %s",
                aggregatedEvents.size(), newInterval, topic));

        //Uplift the statkey to the new interval and put it on the topic
        //We will not be trying to uplift the statKey if we are already at the highest interval
        //so the RTE that cloneAndChangeInterval can throw should never happen
        aggregatedEvents.entrySet().stream()
                .map(entry -> new ProducerRecord<>(
                        topic,
                        entry.getKey().cloneAndChangeInterval(newInterval),
                        entry.getValue()))
                .peek(producerRecord -> LOGGER.trace("Putting record {} on topic {}", producerRecord, topic))
                .forEach(producer::send);

        producer.flush();
    }

    private int getPollTimeout() {
        return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS, 100);
    }

    @Override
    public void stop() {

        //TODO set a flag to initiate a clean stop

    }

    @Override
    public void start() {

        //TODO initiate the polling

    }

//    KafkaStreams buildStream(final StreamsConfig streamsConfig,
//                             final String inputTopic,
//                             final Class<? extends StatAggregate> statAggregateType) {
//
//        //TODO This means changing this prop will require an app restart
//        final int maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
//
//        Serde<StatKey> statKeySerde = StatKeySerde.instance();
//        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();
//
//        KStreamBuilder builder = new KStreamBuilder();
//        KStream<StatKey, StatAggregate> inputStream = builder.stream(statKeySerde, statAggregateSerde, inputTopic);
//
//        Initializer<StatAggregate> aggregateInitializer = createAggregateInitializer(statAggregateType);
//
//        //TODO This needs configuring in properties, probably on a per interval basis so short intervals have a longer time
//        //window duration.
//        //
//        //These time windows have ZERO relation to the interval time buckets. We are simple aggregating our messages
//        //by their keys which have already been truncated down to a time interval into suitably sized (time wise) buckets
//        //Thus if we get N messages in time T with the same statKey, we will get a single statAggregate composed from
//        //N original values. In this case the original value is already of type StatAggregate to confuse matters
//        TimeWindows timeWindows = TimeWindows.of(1000);
//        Windows<TimeWindow> windows = timeWindows.until(0);
//
//        String aggregateStoreName = inputTopic + "-aggregateStore";
//        StateStoreSupplier aggregateStore = Stores.create(aggregateStoreName)
//                .withKeys(statKeySerde)
//                .withValues(statAggregateSerde)
//                .inMemory()
//                .build();
//
////        KStream<Windowed<StatKey>, StatAggregate> windowedAggregates = inputStream
////                .selectKey((statKey, statAggregate) -> statKey.cloneAndTruncateTimeToInterval())
////                .transform()
////                .groupByKey(statKeySerde, statAggregateSerde)
////                .aggregate(
////                        aggregateInitializer,
////                        this::aggregate,
////                        windows,
////                        statAggregateSerde,
////                        "statAggregatesBatches")
////                .toStream();
//
////        windowedAggregates
//
//        /*
//        processor 2 (one instance per time interval - S/M/H/D)
//            .stream
//            .selectKey() //k=name/tagvalues/type/truncatedTime/currentBucket v=aggregate, map the key only - truncate the time to the current bucket
//            .aggregateByKey //aggregate within a tumbling window (period configured per bucket size, maybe)
//            .through //add the current aggregates to a topic (one per stat type and interval) for load into hbase
//            .filter() //currentBucket != largest bucket, to stop further processing
//            .selectKey() //k=name/tagvalues/type/truncatedTime/nextBucket v=aggregate, map the key - move bucket to next biggest
//            .to() //put on topic for this bucket size
//
//         */
//
//
//
//
//
//        return null;
//    }


}
