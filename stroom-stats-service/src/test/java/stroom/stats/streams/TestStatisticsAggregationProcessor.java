package stroom.stats.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.hbase.uid.UID;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.topics.TopicDefinition;
import stroom.stats.streams.topics.TopicDefinitionFactory;
import stroom.stats.test.StatEventKeyHelper;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestStatisticsAggregationProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStatisticsAggregationProcessor.class);
    private static final long DEFAULT_WAIT_TIME = 300L;

    private final MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();
    private final TopicDefinitionFactory topicDefinitionFactory = new TopicDefinitionFactory(mockStroomPropertyService);


    @Mock
    private StatisticsService mockStatisticsService;

    @Captor
    private ArgumentCaptor<Map<StatEventKey, StatAggregate>> aggregatesMapCator;

    private final MockProducer<StatEventKey, StatAggregate> mockProducer = new MockProducer<>();
    private final MockConsumer<StatEventKey, StatAggregate> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final ConsumerFactory consumerFactory = new MockConsumerFactory(mockConsumer);

    @Before
    public void setUp() throws Exception {
        mockStroomPropertyService.setProperty(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 1_000);
        mockStroomPropertyService.setProperty(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_POLL_TIMEOUT_MS, 50);

        setFlushIntervalMs(200);
    }

    private void setFlushIntervalMs(final long flushIntervalMs) {
        mockStroomPropertyService.setProperty(
                StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, flushIntervalMs);
    }

    /**
     * MINUTE -> HOUR
     */
    @Test
    public void testAggregation_sameKey() throws InterruptedException {
        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum processorInterval = EventStoreTimeIntervalEnum.MINUTE;
        final LocalDateTime localDateTime = LocalDateTime.now();

        final List<KeyValue<StatEventKey, StatAggregate>> inputRecords = IntStream.rangeClosed(1, 5)
                .boxed()
                .map(i ->
                        new KeyValue<>(StatEventKeyHelper.buildStatKey(localDateTime, processorInterval),
                                (StatAggregate) new CountAggregate(1L)))
                .collect(Collectors.toList());

        doAggregationTest(statisticType,
                processorInterval,
                inputRecords,
                DEFAULT_WAIT_TIME,
                producerRecords -> {

                    Assertions.assertThat(producerRecords)
                            .hasSize(1);
                    StatEventKey statEventKey = producerRecords.get(0).key();
                    CountAggregate countAggregate = (CountAggregate) producerRecords.get(0).value();

                    Assertions.assertThat(countAggregate.getAggregatedCount())
                            .isEqualTo(inputRecords.size());
                });

        // all input aggs for same key so only one output agg
        Assertions.assertThat(aggregatesMapCator.getValue()).hasSize(1);
    }

    /**
     * FOREVER -> -
     */
    @Test
    public void testAggregation_sameKeyLastInterval() throws InterruptedException {
        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum processorInterval = EventStoreTimeIntervalEnum.LARGEST_INTERVAL;
        final LocalDateTime localDateTime = LocalDateTime.now();

        final List<KeyValue<StatEventKey, StatAggregate>> inputRecords = IntStream.rangeClosed(1, 5)
                .boxed()
                .map(i ->
                        new KeyValue<>(StatEventKeyHelper.buildStatKey(localDateTime, processorInterval),
                                (StatAggregate) new CountAggregate(1L)))
                .collect(Collectors.toList());

        doAggregationTest(statisticType,
                processorInterval,
                inputRecords,
                DEFAULT_WAIT_TIME,
                producerRecords -> {
                    // Input aggs are at largest interval so nothing put on a topic
                    Assertions.assertThat(producerRecords)
                            .hasSize(0);
                });

        // all input aggs for same key so only one output agg
        Assertions.assertThat(aggregatesMapCator.getValue()).hasSize(1);
    }

    @Test
    public void testAggregation_multipleKeys() throws InterruptedException {
        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum processorInterval = EventStoreTimeIntervalEnum.MINUTE;
        int aggregatesPerKey = 3;
        int keyCount = 5;
        int expectedOutputMsgCount = keyCount;
        int expectedAggregatesSentToService = keyCount;
        final LocalDateTime localDateTime = LocalDateTime.now();

        // build input data
        final List<KeyValue<StatEventKey, StatAggregate>> inputRecords = IntStream.rangeClosed(1, keyCount)
                .boxed()
                .flatMap(i -> {
                    final StatEventKey statEventKey = StatEventKeyHelper.buildStatKey(
                            UID.from(new byte[]{3, 0, 0, i.byteValue()}), localDateTime, processorInterval);

                    return IntStream.rangeClosed(1, aggregatesPerKey)
                            .boxed()
                            .map(j ->
                                    new KeyValue<>(
                                            statEventKey,
                                            (StatAggregate) new CountAggregate(1L)));

                })
                .collect(Collectors.toList());

        doAggregationTest(statisticType,
                processorInterval,
                inputRecords,
                DEFAULT_WAIT_TIME,
                producerRecords -> {

                    Assertions.assertThat(producerRecords)
                            .hasSize(expectedOutputMsgCount);
                    StatEventKey statEventKey = producerRecords.get(0).key();
                    producerRecords.forEach(record -> {
                        CountAggregate countAggregate = (CountAggregate) record.value();

                        Assertions.assertThat(countAggregate.getAggregatedCount())
                                .isEqualTo(aggregatesPerKey);
                    });
                });

        // all input aggs for same key so only one output agg
        Assertions.assertThat(aggregatesMapCator.getValue())
                .hasSize(expectedAggregatesSentToService);
    }

    @Test
    public void testAggregation_multipleKeys_multipleFlushes() throws InterruptedException {
        // short flush interval so we get multiple flushes
        setFlushIntervalMs(10);

        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum processorInterval = EventStoreTimeIntervalEnum.MINUTE;
        int aggregatesPerKey = 20;
        int keyCount = 10;
        int expectedMinOutputMsgCount = keyCount;
        int expectedAggregatesSentToService = keyCount;
        long expectedTotalAggregate = aggregatesPerKey * keyCount;
        final LocalDateTime localDateTime = LocalDateTime.now();

        // build input data
        final List<KeyValue<StatEventKey, StatAggregate>> inputRecords = IntStream.rangeClosed(1, keyCount)
                .boxed()
                .flatMap(i -> {
                    final StatEventKey statEventKey = StatEventKeyHelper.buildStatKey(
                            UID.from(new byte[]{3, 0, 0, i.byteValue()}), localDateTime, processorInterval);

                    return IntStream.rangeClosed(1, aggregatesPerKey)
                            .boxed()
                            .map(j ->
                                    new KeyValue<>(
                                            statEventKey,
                                            (StatAggregate) new CountAggregate(1L)));

                })
                .collect(Collectors.toList());

        doAggregationTest(statisticType,
                processorInterval,
                inputRecords,
                1_000L,
                producerRecords -> {

                    Assertions.assertThat(producerRecords.size())
                            .isGreaterThanOrEqualTo(expectedMinOutputMsgCount);
                    StatEventKey statEventKey = producerRecords.get(0).key();

                    Assertions.assertThat(producerRecords.stream()
                            .mapToLong(rec -> ((CountAggregate) rec.value()).getAggregatedCount())
                            .sum())
                            .isEqualTo(expectedTotalAggregate);

                    Assertions
                            .assertThat(producerRecords.stream()
                                    .map(ProducerRecord::key)
                                    .distinct()
                                    .count())
                            .isEqualTo(keyCount);
                });

        // all input aggs for same key so only one output agg
        Assertions
                .assertThat(aggregatesMapCator.getAllValues().stream()
                        .flatMap(map ->
                                map.keySet().stream())
                        .distinct()
                        .count())
                .isEqualTo(keyCount);

        Assertions
                .assertThat(aggregatesMapCator.getAllValues().stream()
                        .mapToLong(map ->
                                map.values().stream()
                                        .mapToLong(agg ->
                                                ((CountAggregate) agg).getAggregatedCount())
                                        .sum())
                        .sum())
                .isEqualTo(expectedTotalAggregate);
    }

    private void doAggregationTest(final StatisticType statisticType,
                                   final EventStoreTimeIntervalEnum processorInterval,
                                   final List<KeyValue<StatEventKey, StatAggregate>> inputRecords,
                                   final long waitTimeMs,
                                   final java.util.function.Consumer<List<ProducerRecord<StatEventKey, StatAggregate>>> outputRecordsConsumer)
            throws InterruptedException {

        TopicDefinition<StatEventKey, StatAggregate> aggregatesTopic = topicDefinitionFactory.getAggregatesTopic(
                statisticType,
                processorInterval);
        Optional<EventStoreTimeIntervalEnum> optNextInterval = EventStoreTimeIntervalEnum.getNextBiggest(processorInterval);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final StatisticsAggregationProcessor statisticsAggregationProcessor = new StatisticsAggregationProcessor(
                topicDefinitionFactory,
                mockStatisticsService,
                mockStroomPropertyService,
                statisticType,
                processorInterval,
                mockProducer,
                executorService,
                consumerFactory,
                1);

        int partition = 0;
        int recordCount = inputRecords.size();

        TopicPartition topicPartition = new TopicPartition(aggregatesTopic.getName(), partition);
        Map<TopicPartition, Long> partitionsBeginningMap = new HashMap<>();
        partitionsBeginningMap.put(topicPartition, 0L);
        Map<TopicPartition, Long> partitionsEndMap = new HashMap<>();
        partitionsEndMap.put(topicPartition, recordCount - 1L);

        mockConsumer.updatePartitions(
                aggregatesTopic.getName(),
                Collections.singletonList(
                        new PartitionInfo(
                                aggregatesTopic.getName(),
                                partition,
                                null,
                                new Node[0],
                                new Node[0])));

        statisticsAggregationProcessor.start();

        // tiny sleep to give the processor a chance to subscribe to the topic
        Thread.sleep(50);

        LOGGER.info("Health {}", statisticsAggregationProcessor.getHealth());
        mockConsumer.updateBeginningOffsets(partitionsBeginningMap);
        mockConsumer.updateEndOffsets(partitionsEndMap);
        mockConsumer.rebalance(Collections.singletonList(topicPartition));

        final AtomicLong offset = new AtomicLong(0);
        inputRecords.stream()
                .map(keyValue ->
                        new ConsumerRecord<>(
                                aggregatesTopic.getName(),
                                partition,
                                offset.incrementAndGet(),
                                keyValue.key,
                                keyValue.value))
                .forEach(record -> {
                    LOGGER.info("Adding record {}", record);
                    mockConsumer.addRecord(record);
                });

        // Sleep long enough for the polls to happen and for the aggregator to be flushed
        Thread.sleep(waitTimeMs);

        // Ensure any unflushed aggregates are dealt with
        statisticsAggregationProcessor.stop();

        mockProducer.history()
                .stream()
                .forEach(record -> {
                    LOGGER.info("Seen record {}", record);
                });

        // if there is a next bigger interval then check the messages on the topic are for the correct
        // interval
        if (optNextInterval.isPresent()) {
            final TopicDefinition<StatEventKey, StatAggregate> nextIntervalTopic = topicDefinitionFactory.getAggregatesTopic(
                    statisticType,
                    optNextInterval.get());

            Assertions.assertThat(mockProducer.history().stream().map(record -> record.key().getInterval()))
                    .containsOnly(optNextInterval.get());

            Assertions.assertThat(mockProducer.history().stream().map(ProducerRecord::topic))
                    .containsOnly(nextIntervalTopic.getName());

        } else {
            // no next bigger interval so no msgs on topic
            Assertions.assertThat(mockProducer.history()).hasSize(0);
        }

        verify(mockStatisticsService, atLeastOnce())
                .putAggregatedEvents(
                        Mockito.eq(StatisticType.COUNT),
                        Mockito.eq(processorInterval),
                        aggregatesMapCator.capture());

        outputRecordsConsumer.accept(mockProducer.history());
    }

    private static class MockConsumerFactory implements ConsumerFactory {

        private final MockConsumer<StatEventKey, StatAggregate> consumer;

        private MockConsumerFactory(final MockConsumer<StatEventKey, StatAggregate> consumer) {
            this.consumer = consumer;
        }

        @Override
        public <K, V> Consumer<K, V> createConsumer(final String groupId,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde) {
            return (Consumer<K, V>) consumer;
        }
    }
}