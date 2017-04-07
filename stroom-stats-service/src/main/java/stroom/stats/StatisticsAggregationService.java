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

package stroom.stats;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.mixins.Startable;
import stroom.stats.mixins.Stoppable;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.StatisticsAggregationProcessor;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Singleton
public class StatisticsAggregationService implements Startable, Stoppable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsAggregationService.class);

    private final StroomPropertyService stroomPropertyService;
    private final StatisticsService statisticsService;
    private final List<StatisticsAggregationProcessor> processors = new ArrayList<>();

    private final KafkaProducer<StatKey, StatAggregate> kafkaProducer;
    private final ExecutorService executorService;

    @Inject
    public StatisticsAggregationService(final StroomPropertyService stroomPropertyService,
                                        final StatisticsService statisticsService) {

        this.stroomPropertyService = stroomPropertyService;
        this.statisticsService = statisticsService;

        //producer is thread safe so hold a single instance and share it with all processors
        //this assumes all processor instances have the same producer config
        kafkaProducer = buildProducer();

        //hold an instance of the executorService in case we want to query it for a health check
        executorService = buildProcessors();
    }

    @Override
    public void start() {

        LOGGER.info("Starting the Statistics Aggregation Service");

        processors.forEach(StatisticsAggregationProcessor::start);
    }

    @Override
    public void stop() {

        LOGGER.info("Stopping the Statistics Aggregation Service");

        processors.forEach(StatisticsAggregationProcessor::stop);
    }

    private ExecutorService buildProcessors() {

        //TODO currently each processor will spawn a thread to consume from the appropriate topic,
        //so 8 threads.  Long term we will want finer control, e.g. more threads for Count stats
        //and more for the finer granularities

        //TODO configure the instance count on a per type and interval basis as some intervals/types will need
        //more processing than others
        int instanceCount = 1;
        int processorCount = StatisticType.values().length * EventStoreTimeIntervalEnum.values().length * instanceCount;

        ExecutorService executorService = Executors.newFixedThreadPool(processorCount);

        for (StatisticType statisticType : StatisticType.values()) {
            for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
                for (int instanceId = 0; instanceId < instanceCount; instanceId++) {

                    StatisticsAggregationProcessor processor = new StatisticsAggregationProcessor(
                            statisticsService,
                            stroomPropertyService,
                            statisticType,
                            interval,
                            kafkaProducer,
                            executorService,
                            instanceId);

                    processors.add(processor);
                }
            }
        }
        return executorService;
    }

    private KafkaProducer<StatKey, StatAggregate> buildProducer() {

        //Configure the producers
        Map<String, Object> producerProps = getProducerProps();

        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        return new KafkaProducer<>(
                producerProps,
                    statKeySerde.serializer(),
                    statAggregateSerde.serializer());

    }

    private Map<String, Object> getProducerProps() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG,
                stroomPropertyService.getIntProperty(
                        StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10_000));
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 50_000_000);
        return producerProps;
    }
}
