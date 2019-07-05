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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UID;
import stroom.stats.partitions.StatEventKeyPartitioner;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.StatisticsAggregationProcessor;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatEventKeySerde;
import stroom.stats.streams.topics.TopicDefinition;
import stroom.stats.streams.topics.TopicDefinitionFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StatisticsAggregationServiceIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsAggregationServiceIT.class);

    private static final StatisticType WORKING_STAT_TYPE = StatisticType.COUNT;
    private static final EventStoreTimeIntervalEnum WORKING_INTERVAL = EventStoreTimeIntervalEnum.DAY;

    private MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();

    private MockUniqueIdCache mockUniqueIdCache = new MockUniqueIdCache();

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private StatisticsService mockStatisticsService;

    @Captor
    private ArgumentCaptor<Map<StatEventKey, StatAggregate>> aggregatesMapCaptor;

    @Captor
    private ArgumentCaptor<StatisticType> statTypeCaptor;

    @Captor
    private ArgumentCaptor<EventStoreTimeIntervalEnum> intervalCaptor;

    private TopicDefinitionFactory topicDefinitionFactory = new TopicDefinitionFactory(mockStroomPropertyService);

    @Test
    public void testAggregation() throws InterruptedException {

        LOGGER.info("Starting test");

        setProperties();

        StatisticsAggregationService statisticsAggregationService = new StatisticsAggregationService(
                mockStroomPropertyService,
                topicDefinitionFactory, mockStatisticsService);

        statisticsAggregationService.start();

        //give the consumers a bit of a chance to spin up before firing data at them, else they will miss
        //records do to autoOffsetRest being set to latest
        Thread.sleep(1_000);

        TopicDefinition<StatEventKey, StatAggregate> inputTopic = topicDefinitionFactory.getAggregatesTopic(
                WORKING_STAT_TYPE,
                WORKING_INTERVAL);

        String statName = "MyStat-" + Instant.now().toString();
        UID statNameUid = mockUniqueIdCache.getOrCreateId(statName);
        ZonedDateTime baseTime = ZonedDateTime.now(ZoneOffset.UTC);

        final KafkaProducer<StatEventKey, StatAggregate> kafkaProducer = buildKafkaProducer(mockStroomPropertyService);

        int iterations = 50_000;

        LongAdder msgPutCounter = new LongAdder();

        List<Future<RecordMetadata>> futures = Collections.synchronizedList(new ArrayList<>());

        //send all the msgs to kafka (async)
        IntStream.rangeClosed(1, iterations)
                .parallel()
                .forEach(i -> {
                    Future<RecordMetadata> future = kafkaProducer.send(buildProducerRecord(
                            inputTopic.getName(),
                            statNameUid,
                            baseTime.plus(i, ChronoUnit.MINUTES)));
                    futures.add(future);
                    msgPutCounter.increment();
                });

        //wait for all send ops to complete
        List<RecordMetadata> metas = futures.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interupted");
                    } catch (ExecutionException e) {
                        throw new RuntimeException(String.format("Exception in send, %s", e.getMessage()), e);
                    }
                })
                .collect(Collectors.toList());

        //Capture offset summaries by partition
        Map<Integer, LongSummaryStatistics> summary = metas.stream()
                .collect(Collectors.groupingBy(
                        RecordMetadata::partition,
                        Collectors.summarizingLong(RecordMetadata::offset)));

        //wait a bit before we start checking the processed data
        Thread.sleep(1_000);

        long aggSum = 0;

        Instant timeoutTime = baseTime.plusSeconds(10).toInstant();

        //loop until we get the sum of aggregates that we expect
        //each msg is a milli apart so will be aggregated into the same DAY bucket and as each agg value is 1
        //we should get a sum of all agg values at the end equal to the number of iterations
        while (aggSum < iterations && Instant.now().isBefore(timeoutTime)) {

            aggSum = getAggSum(statNameUid);

            LOGGER.debug("aggSum {}", aggSum);

            Thread.sleep(400);
        }

        statisticsAggregationService.stop();

        LOGGER.info("groupId prefix {}", mockStroomPropertyService.getPropertyOrThrow(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX));

        LOGGER.info("baseTime {}", baseTime.toString());
        LOGGER.info("msgPutCount {}", msgPutCounter);
//        LOGGER.info("msgCount {}", StatisticsAggregationProcessor.msgCounter.sum());
//        LOGGER.info("minTimestamp {}", Instant.ofEpochMilli(
//                StatisticsAggregationProcessor.minTimestamp.get()).toString());

        LOGGER.info("Producer offsets summary");
        summary.keySet().stream()
                .sorted()
                .map(k -> k + " - " + summary.get(k).toString())
                .forEach(LOGGER::info);

        LOGGER.info("Consumer offsets summary");
//        StatisticsAggregationProcessor.consumerRecords.entrySet().stream()
//                .sorted(Comparator.comparingInt(Map.Entry::getKey))
//                .map(entry ->
//                    entry.getKey() + " - " + entry.getValue().stream()
//                            .collect(Collectors.summarizingLong(ConsumerRecord::offset)))
//                .forEach(LOGGER::info);

        Assertions.assertThat(aggSum).isEqualTo(iterations);
    }

    /**
     * Get the sum of all count aggregate values passed to the {@link StatisticsService} so far, ensuring the
     * statKey is for out statName
     */
    private long getAggSum(UID statNameUid) {


        Mockito.verify(mockStatisticsService, Mockito.atLeast(0)).putAggregatedEvents(statTypeCaptor.capture(),
                intervalCaptor.capture(),
                aggregatesMapCaptor.capture());

        aggregatesMapCaptor.getAllValues().forEach(map ->
                map.forEach((k, v) -> LOGGER.debug("{} - {} ", k, v)));

        return aggregatesMapCaptor.getAllValues().stream()
                .flatMap(map -> map.entrySet().stream().map(entry -> new KeyValue<>(entry.getKey(), entry.getValue())))
                .filter(kv -> kv.key.getStatUuid().equals(statNameUid))
                .mapToLong(kv -> ((CountAggregate) kv.value).getAggregatedCount())
                .sum();
    }

    private void setProperties() {
        mockStroomPropertyService.setProperty(StatisticsAggregationService.PROP_KEY_THREADS_PER_INTERVAL_AND_TYPE, 4);
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 1_000);
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10_000);
        //because the mockUniqueIdCache is not persistent we will always get ID 0001 for the statUuid regardless of what
        //it is.  Therefore we need to set a unique groupId for the consumer inside the processor so it doesn't get
        //msgs from kafka from a previous run
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX,
                "StatAggSrvIT-" + Instant.now().toEpochMilli());

        //for testing we only ever want to grab from the latest offset when we spin up a new test to avoid consuming records
        //from a previous run, does mean we have to spin up the consumer(s) before putting records on a topic
        mockStroomPropertyService.setProperty(StatisticsIngestService.PROP_KEY_KAFKA_AUTO_OFFSET_RESET, "latest");
    }

    private static KafkaProducer<StatEventKey, StatAggregate> buildKafkaProducer(StroomPropertyService stroomPropertyService) {

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5_000_000);
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StatEventKeyPartitioner.class);

        Serde<StatEventKey> statKeySerde = StatEventKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        return new KafkaProducer<>(producerProps, statKeySerde.serializer(), statAggregateSerde.serializer());
    }

    private ProducerRecord<StatEventKey, StatAggregate> buildProducerRecord(
            String topic,
            UID statNameUid,
            ZonedDateTime time) {

        StatEventKey statEventKey = new StatEventKey(statNameUid, RollUpBitMask.ZERO_MASK, WORKING_INTERVAL, time.toInstant().toEpochMilli());
        StatAggregate statAggregate = new CountAggregate(1L);
        return new ProducerRecord<>(topic, statEventKey, statAggregate);
    }
}