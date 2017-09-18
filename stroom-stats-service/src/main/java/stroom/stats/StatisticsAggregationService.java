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

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.partitions.StatEventKeyPartitioner;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.StatisticsAggregationProcessor;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatEventKeySerde;
import stroom.stats.util.HasRunState;
import stroom.stats.util.Startable;
import stroom.stats.util.Stoppable;
import stroom.stats.util.healthchecks.HasHealthCheck;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@Singleton
public class StatisticsAggregationService implements Startable, Stoppable, HasRunState, HasHealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsAggregationService.class);

    public static final String PROP_KEY_THREADS_PER_INTERVAL_AND_TYPE = "stroom.stats.aggregation.threadsPerIntervalAndType";

    public static final long TIMEOUT_SECS = 120;

    private final StroomPropertyService stroomPropertyService;
    private final StatisticsService statisticsService;

    private final List<StatisticsAggregationProcessor> processors = Collections.synchronizedList(new ArrayList<>());

    //producer is thread safe so hold a single instance and share it with all processors
    //this assumes all processor instances have the same producer config
    private volatile KafkaProducer<StatEventKey, StatAggregate> kafkaProducer;
    private volatile ExecutorService executorService;

    private HasRunState.RunState runState = HasRunState.RunState.STOPPED;

    //used for thread synchronization
    private final Object startStopMonitor = new Object();

    @Inject
    public StatisticsAggregationService(final StroomPropertyService stroomPropertyService,
                                        final StatisticsService statisticsService) {

        LOGGER.debug("Initialising {}", this.getClass().getName());

        this.stroomPropertyService = stroomPropertyService;
        this.statisticsService = statisticsService;
    }

    @Override
    public void start() {

        synchronized (startStopMonitor) {
            runState = RunState.STARTING;
            LOGGER.info("Starting the Statistics Aggregation Service");

            //shared by all processors
            kafkaProducer = buildProducer();

            //hold an instance of the executorService in case we want to query it for a health check
            //build processors on start so we can start/stop to later the processor counts if we need to
            executorService = buildProcessors();

            runOnAllProcessorsAsyncThenWait("start", StatisticsAggregationProcessor::start);

            runState = RunState.RUNNING;
        }
    }

    @Override
    public void stop() {

        synchronized (startStopMonitor) {
            runState = RunState.STOPPING;
            LOGGER.info("Stopping the Statistics Aggregation Service");

            runOnAllProcessorsAsyncThenWait("stop", StatisticsAggregationProcessor::stop);
            //dereference all the processors
            processors.clear();

            //have to shut this down second as the processor shutdown will probably flush more items to the producer
            if (kafkaProducer != null) {
                kafkaProducer.close(TIMEOUT_SECS, TimeUnit.SECONDS);
                kafkaProducer = null;
            }
            runState = RunState.STOPPED;
        }
    }

    private void runOnAllProcessorsAsyncThenWait(final String actionDescription,
                                                 final Consumer<StatisticsAggregationProcessor> processorConsumer) {

        Preconditions.checkArgument(!processors.isEmpty(), "Attempting to run action on all processors when there are none");

        //stop each processor as an async task, collecting all the futures
        CompletableFuture<Void>[] completableFutures = processors.stream()
                .map(processor -> CompletableFuture.runAsync(() -> processorConsumer.accept(processor)))
                .toArray(size -> new CompletableFuture[size]);

        try {
            //wait for all tasks to complete before proceeding
            CompletableFuture.allOf(completableFutures).get(TIMEOUT_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread interrupted performing {} on all processors", actionDescription, e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(String.format("Error while performing %s on all processors", actionDescription), e);
        } catch (TimeoutException e) {
            LOGGER.error("Unable to perform {} on all processors after {}s", actionDescription, TIMEOUT_SECS, e);
        }
    }

    private ExecutorService buildProcessors() {

        //TODO currently each processor will spawn a thread to consume from the appropriate topic,
        //so 8 threads.  Long term we will want finer control, e.g. more threads for Count stats
        //and more for the finer granularities

        //TODO configure the instance count on a per type and interval basis as some intervals/types will need
        //more processing than others
        int instanceCount = stroomPropertyService.getIntProperty(PROP_KEY_THREADS_PER_INTERVAL_AND_TYPE, 1);

        int processorCount = StatisticType.values().length * EventStoreTimeIntervalEnum.values().length * instanceCount;

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("agg-proc-thread-%d")
                .build();
        ExecutorService executorService = Executors.newFixedThreadPool(processorCount, namedThreadFactory);

        //create all the processor instances and hold their references
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

    private KafkaProducer<StatEventKey, StatAggregate> buildProducer() {

        //Configure the producers
        Map<String, Object> producerProps = getProducerProps();

        Serde<StatEventKey> statKeySerde = StatEventKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        return new KafkaProducer<>(producerProps, statKeySerde.serializer(), statAggregateSerde.serializer());
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
        //use a custom partitioner to benefit from the already cached hashcode in the statkey
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StatEventKeyPartitioner.class);
        return producerProps;
    }

    @Override
    public HealthCheck.Result getHealth() {
        HealthCheck.ResultBuilder builder = HealthCheck.Result.builder();
        long nonRunningProcessorCount = processors.stream()
                .filter(processor -> !processor.getRunState().equals(RunState.RUNNING))
                .count();
        if (!runState.equals(RunState.RUNNING) || nonRunningProcessorCount > 0) {
            builder.unhealthy();
        } else {
            builder.healthy();
        }
        builder.withDetail("runState", runState.name());
        builder.withDetail("processorCount", processors.size());
        builder.withDetail("processors", processors.stream()
                .collect(HasHealthCheck.buildTreeMapCollector(
                        StatisticsAggregationProcessor::getName,
                        StatisticsAggregationProcessor::produceHealthCheckSummary)));

        return builder.build();
    }

    public List<HasHealthCheck> getHealthCheckProviders() {
        List<HasHealthCheck> healthCheckProviders = new ArrayList<>();
        processors.forEach(healthCheckProviders::add);
        return healthCheckProviders;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public RunState getRunState() {
        return runState;
    }
}
