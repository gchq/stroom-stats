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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class StatisticsAggregationProcessor {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsAggregationProcessor.class);

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


    KafkaStreams buildStream(final StreamsConfig streamsConfig,
                             final String inputTopic,
                             final Class<? extends StatAggregate> statAggregateType) {

        //TODO This means changing this prop will require an app restart
        final int maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);

        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<StatKey, StatAggregate> inputStream = builder.stream(statKeySerde, statAggregateSerde, inputTopic);

        Initializer<StatAggregate> aggregateInitializer = createAggregateInitializer(statAggregateType);

        //TODO This needs configuring in properties, probably on a per interval basis so short intervals have a longer time
        //window duration.
        //
        //These time windows have ZERO relation to the interval time buckets. We are simple aggregating our messages
        //by their keys which have already been truncated down to a time interval into suitably sized (time wise) buckets
        //Thus if we get N messages in time T with the same statKey, we will get a single statAggregate composed from
        //N original values. In this case the original value is already of type StatAggregate to confuse matters
        TimeWindows timeWindows = TimeWindows.of(1000);
        Windows<TimeWindow> windows = timeWindows.until(0);

        String aggregateStoreName = inputTopic + "-aggregateStore";
        StateStoreSupplier aggregateStore = Stores.create(aggregateStoreName)
                .withKeys(statKeySerde)
                .withValues(statAggregateSerde)
                .inMemory()
                .build();

//        KStream<Windowed<StatKey>, StatAggregate> windowedAggregates = inputStream
//                .selectKey((statKey, statAggregate) -> statKey.cloneAndTruncateTimeToInterval())
//                .transform()
//                .groupByKey(statKeySerde, statAggregateSerde)
//                .aggregate(
//                        aggregateInitializer,
//                        this::aggregate,
//                        windows,
//                        statAggregateSerde,
//                        "statAggregatesBatches")
//                .toStream();

//        windowedAggregates

        /*
        processor 2 (one instance per time interval - S/M/H/D)
            .stream
            .selectKey() //k=name/tagvalues/type/truncatedTime/currentBucket v=aggregate, map the key only - truncate the time to the current bucket
            .aggregateByKey //aggregate within a tumbling window (period configured per bucket size, maybe)
            .through //add the current aggregates to a topic (one per stat type and interval) for load into hbase
            .filter() //currentBucket != largest bucket, to stop further processing
            .selectKey() //k=name/tagvalues/type/truncatedTime/nextBucket v=aggregate, map the key - move bucket to next biggest
            .to() //put on topic for this bucket size

         */





        return null;
    }



    public void startProcessor(final ConsumerConfig consumerConfig,
                               final ProducerConfig producerConfig,
                               final String inputTopic,
                               final String nextIntervalTopic,
                               final StatisticType statisticType,
                               final EventStoreTimeIntervalEnum aggregationInterval,
                               final Class<? extends StatAggregate> statAggregateType) {

        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<StatKey, StatAggregate> kafkaConsumer = new KafkaConsumer<>(consumerConfig.originals(),
                    statKeySerde.deserializer(),
                    statAggregateSerde.deserializer());

            //Subscribe to all perm topics for this stat type
//            List<String> topics = ROLLUP_TOPICS_MAP.entrySet().stream()
//                    .flatMap(entry -> entry.getValue().stream())
//                    .collect(Collectors.toList());

            LOGGER.info("Starting consumer for type {}, interval {}, inputTopic {}, nextIntervalTopic",
                    statisticType, aggregationInterval, inputTopic, nextIntervalTopic);
            kafkaConsumer.subscribe(Collections.singletonList(inputTopic));

            List<ConsumerRecord<StatKey, StatAggregate>> buffer = new ArrayList<>();

            try {
                while (true) {
                    try {
                        ConsumerRecords<StatKey, StatAggregate> records = kafkaConsumer.poll(1000);

                        //TODO WIP


                        for (ConsumerRecord<StatKey, StatAggregate> record : records) {

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
