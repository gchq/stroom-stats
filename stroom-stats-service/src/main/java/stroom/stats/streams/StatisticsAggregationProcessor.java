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
 *
 *
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class StatisticsAggregationProcessor {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsAggregationProcessor.class);

    public static final String PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE = "stroom.stats.aggregation.minBatchSize";
    public static final String PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS = "stroom.stats.aggregation.maxFlushIntervalMs";

    private final StatisticsService statisticsService;
    private final StroomPropertyService stroomPropertyService;
    private final int maxEventIds;

    //TODO probably should be called ...ProcessorBuilder as it is not a processor but the builder of one
    @Inject
    public StatisticsAggregationProcessor(final StatisticsService statisticsService, final StroomPropertyService stroomPropertyService) {
        this.statisticsService = statisticsService;
        this.stroomPropertyService = stroomPropertyService;
        maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
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

    public void startProcessor(final Map<String, Object> consumerProps,
                               final Map<String, Object> producerProps,
                               final String inputTopic,
                               final Optional<String> nextIntervalTopic,
                               final StatisticType statisticType,
                               final EventStoreTimeIntervalEnum aggregationInterval) {

        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        int maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
        int maxFlushIntervalMs = stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 60_000);
        long minBatchSize = stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 1_000);

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

            LOGGER.info("Starting consumer/producer for type {}, interval {}, inputTopic {}, nextIntervalTopic",
                    statisticType, aggregationInterval, inputTopic, nextIntervalTopic.orElse("Empty"));

            kafkaConsumer.subscribe(Collections.singletonList(inputTopic));

            StatAggregator statAggregator = new StatAggregator((int) (minBatchSize * 1.2), maxEventIds, aggregationInterval);

            final Instant lastCommitTime = Instant.now();

            try {
                while (true) {
                    try {
                        ConsumerRecords<StatKey, StatAggregate> records = kafkaConsumer.poll(1000);

                        LOGGER.trace(() -> String.format("Received %s records from topic %s", records.count(), inputTopic));

                        for (ConsumerRecord<StatKey, StatAggregate> record : records) {
                            statAggregator.add(record.key(), record.value());
                        }

                        //flush if the aggregator is too big or it has been too long since the last flush
                        if (statAggregator.size() >= minBatchSize || Duration.between(lastCommitTime, Instant.now()).toMillis() > maxFlushIntervalMs) {

                            //flush all the aggregated stats down to the StatStore and onto the next biggest interval topic
                            //(if there is one) for coarser aggregation
                            flushToStatStore(statisticType, statAggregator);
                            if (nextIntervalTopic.isPresent()) {
                                flushToTopic(statAggregator, nextIntervalTopic.get(), kafkaProducer);
                            }
                            statAggregator.clear();
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


    private void flushToStatStore(final StatisticType statisticType, final StatAggregator statAggregator) {
        List<AggregatedEvent> aggregatedEvents = statAggregator.getAll();
        statisticsService.putAggregatedEvents(statisticType, statAggregator.getAggregationInterval(), aggregatedEvents);
    }

    private void flushToTopic(final StatAggregator statAggregator,
                              final String topic,
                              final KafkaProducer<StatKey, StatAggregate> producer) {
        Preconditions.checkNotNull(statAggregator);
        Preconditions.checkNotNull(producer);

        statAggregator.stream()
                .map(aggregatedEvent -> new ProducerRecord<>(topic, aggregatedEvent.getStatKey(), aggregatedEvent.getStatAggregate()))
                .forEach(producer::send);

        producer.flush();
    }



    StatAggregate aggregate(final StatKey statKey, final StatAggregate originalValue, final StatAggregate cumulativeAggregate) {
        return cumulativeAggregate.aggregate(originalValue, maxEventIds);
    }

    Initializer<StatAggregate> createAggregateInitializer(Class<? extends StatAggregate> statAggregateType) {
        return () -> {
                    try {
                        return statAggregateType.getDeclaredConstructor().newInstance();
                    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                        throw new RuntimeException(String.format("Unable to create a new instance of %s", statAggregateType.getName()), e);
                    }
        };
    }

    private static class StatAggregator {
        private final Map<StatKey, StatAggregate> buffer;
        private final int maxEventIds;
        private final EventStoreTimeIntervalEnum aggregationInterval;

        public StatAggregator(final int expectedSize, final int maxEventIds, final EventStoreTimeIntervalEnum aggregationInterval) {
            //initial size to avoid it rehashing
            this.buffer = new HashMap<>((int)Math.ceil(expectedSize / 0.75));
            this.maxEventIds = maxEventIds;
            this.aggregationInterval = aggregationInterval;
        }

        public void add(final StatKey statKey, final StatAggregate statAggregate){
            statKey.cloneAndTruncateTimeToInterval(aggregationInterval);

            //aggregate the passed aggregate and key into the existing aggregates
            buffer.merge(
                    statKey,
                    statAggregate,
                    (existingAgg, newAgg) -> existingAgg.aggregate(newAgg, maxEventIds));
        }

        public int size() {
            return buffer.size();
        }

        public void clear() {
            buffer.clear();
        }

        public EventStoreTimeIntervalEnum getAggregationInterval() {
            return aggregationInterval;
        }

        public Set<Map.Entry<StatKey, StatAggregate>> entrySet() {
            return buffer.entrySet();
        }


        public Stream<AggregatedEvent> stream() {
            return buffer.entrySet().stream()
                    .map(entry -> new AggregatedEvent(entry.getKey(), entry.getValue()));

        }
        public List<AggregatedEvent> getAll() {
            return stream().collect(Collectors.toList());
        }
    }

    private static class AggregationTransformer implements Transformer<StatKey, StatAggregate, KeyValue<StatKey, StatAggregate>> {

        @Override
        public void init(final ProcessorContext context) {

        }

        @Override
        public KeyValue<StatKey, StatAggregate> transform(final StatKey key, final StatAggregate value) {
            return null;
        }

        @Override
        public KeyValue<StatKey, StatAggregate> punctuate(final long timestamp) {
            return null;
        }

        @Override
        public void close() {

        }
    }






}
