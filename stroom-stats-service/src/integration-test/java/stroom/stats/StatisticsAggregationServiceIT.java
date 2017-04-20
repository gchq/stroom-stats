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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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
import stroom.stats.partitions.StatKeyPartitioner;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.StatisticsAggregationProcessor;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.streams.TopicNameFactory;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.util.thread.ThreadUtil;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
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
    StatisticsService mockStatisticsService;

    @Test
    public void testAggregation() {

        LOGGER.info("Starting test");

        setProperties();

        StatisticsAggregationService statisticsAggregationService = new StatisticsAggregationService(
                mockStroomPropertyService,
                mockStatisticsService);

        statisticsAggregationService.start();

        String inputTopic = TopicNameFactory.getIntervalTopicName(
                mockStroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX),
                WORKING_STAT_TYPE,
                WORKING_INTERVAL);

        String statName = "MyStat-" + Instant.now().toString();
        UID statNameUid = mockUniqueIdCache.getOrCreateId(statName);
        ZonedDateTime baseTime = ZonedDateTime.now(ZoneOffset.UTC);

        final KafkaProducer<StatKey, StatAggregate> kafkaProducer = buildKafkaProducer(mockStroomPropertyService);

        int iterations = 50_000;

        //send all the msgs to kafka
        IntStream.rangeClosed(1, iterations).forEachOrdered(i -> {
            kafkaProducer.send(buildProducerRecord(inputTopic, statNameUid, baseTime.plus(i, ChronoUnit.MINUTES)));
        });

        ThreadUtil.sleep(1_000);

        long aggSum = 0;

        //loop until we get the sum of aggregates that we expect
        //each msg is a milli apart so will be aggregated into the same DAY bucket and as each agg value is 1
        //we should get a sum of all agg values at the end equal to the number of iterations
        while (aggSum < iterations) {

            aggSum = getAggSum(statNameUid);

            LOGGER.debug("aggSum {}", aggSum);

            ThreadUtil.sleep(400);
        }

        statisticsAggregationService.stop();

        aggSum = getAggSum(statNameUid);

        Assertions.assertThat(aggSum).isEqualTo(iterations);
    }

    /**
     * Get the sum of all count aggregate values passed to the {@link StatisticsService} so far, ensuring the
     * statKey is for out statName
     */
    private long getAggSum(UID statNameUid) {

        ArgumentCaptor<StatisticType> statTypeCaptor = ArgumentCaptor.forClass(StatisticType.class);
        ArgumentCaptor<EventStoreTimeIntervalEnum> intervalCaptor = ArgumentCaptor.forClass(EventStoreTimeIntervalEnum.class);
        ArgumentCaptor<Map<StatKey, StatAggregate>> aggregatesMapCaptor = ArgumentCaptor.forClass(Map.class);

        Mockito.verify(mockStatisticsService, Mockito.atLeast(0)).putAggregatedEvents(statTypeCaptor.capture(),
                intervalCaptor.capture(),
                aggregatesMapCaptor.capture());

        aggregatesMapCaptor.getAllValues().forEach(map ->
                map.forEach((k, v) -> LOGGER.debug("{} - {} ", k, v)));

        return aggregatesMapCaptor.getAllValues().stream()
                .flatMap(map -> map.entrySet().stream().map(entry -> new KeyValue<>(entry.getKey(), entry.getValue())))
                .filter(kv -> kv.key.getStatName().equals(statNameUid))
                .mapToLong(kv -> ((CountAggregate) kv.value).getAggregatedCount())
                .sum();
    }

    private void setProperties() {
        mockStroomPropertyService.setProperty(StatisticsAggregationService.PROP_KEY_THREADS_PER_INTERVAL_AND_TYPE, 1);
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 500);
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10_000);
        //because the mockUniqueIdCache is not persistent we will always get ID 0001 for the statname regardless of what
        //it is.  Therefore we need to set a unique groupId for the consumer inside the processor so it doesn't get
        //msgs from kafka from a previous run
        mockStroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATION_PROCESSOR_APP_ID_PREFIX,
                "StatAggSrvIT-" + Instant.now().toEpochMilli());
    }

    private static KafkaProducer<StatKey, StatAggregate> buildKafkaProducer(StroomPropertyService stroomPropertyService) {

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5_000_000);
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StatKeyPartitioner.class);

        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        return new KafkaProducer<>(producerProps, statKeySerde.serializer(), statAggregateSerde.serializer());
    }

    private ProducerRecord<StatKey, StatAggregate> buildProducerRecord(
            String topic,
            UID statNameUid,
            ZonedDateTime time) {

        StatKey statKey = new StatKey(statNameUid, RollUpBitMask.ZERO_MASK, WORKING_INTERVAL, time.toInstant().toEpochMilli());
        StatAggregate statAggregate = new CountAggregate(1L);
        return new ProducerRecord<>(topic, statKey, statAggregate);
    }
}