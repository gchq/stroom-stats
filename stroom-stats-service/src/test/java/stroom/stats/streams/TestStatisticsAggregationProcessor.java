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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestStatisticsAggregationProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStatisticsAggregationProcessor.class);

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

    @Test
    public void testAggregation_sameKey() throws InterruptedException {
        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum processorInterval = EventStoreTimeIntervalEnum.MINUTE;
        EventStoreTimeIntervalEnum nextInterval = EventStoreTimeIntervalEnum.getNextBiggest(processorInterval).get();
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
                producerRecords -> {

                    Assertions.assertThat(producerRecords)
                            .hasSize(1);
                    StatEventKey statEventKey = producerRecords.get(0).key();
                    CountAggregate countAggregate = (CountAggregate) producerRecords.get(0).value();

                    Assertions.assertThat(countAggregate.getAggregatedCount())
                            .isEqualTo(inputRecords.size());
                    // The aggregate should be put on a topic for the next interval
                    Assertions.assertThat(statEventKey.getInterval())
                            .isEqualTo(nextInterval);

                    Assertions.assertThat(producerRecords.get(0).topic())
                            .isEqualTo(topicDefinitionFactory.getAggregatesTopic(statisticType, nextInterval).getName());
                });

        verify(mockStatisticsService, times(1))
                .putAggregatedEvents(
                        Mockito.eq(StatisticType.COUNT),
                        Mockito.eq(processorInterval),
                        aggregatesMapCator.capture());

        Assertions.assertThat(aggregatesMapCator.getValue()).hasSize(1);
    }

    private void doAggregationTest(final StatisticType statisticType,
                                   final EventStoreTimeIntervalEnum processorInterval,
                                   final List<KeyValue<StatEventKey, StatAggregate>> inputRecords,
                                   final java.util.function.Consumer<List<ProducerRecord<StatEventKey, StatAggregate>>> outputRecordsConsumer)
            throws InterruptedException {

        TopicDefinition<StatEventKey, StatAggregate> aggregatesTopic = topicDefinitionFactory.getAggregatesTopic(
                statisticType,
                processorInterval);

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

        Thread.sleep(500);

        // Ensure any unflushed aggregates are dealt with
        statisticsAggregationProcessor.stop();

        mockProducer.history()
                .stream()
                .forEach(record -> {
                    LOGGER.info("Seen record {}", record);
                });

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