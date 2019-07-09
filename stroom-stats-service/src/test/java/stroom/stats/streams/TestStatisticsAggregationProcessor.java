package stroom.stats.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.topics.TopicDefinitionFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(MockitoJUnitRunner.class)
public class TestStatisticsAggregationProcessor {

    private final MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();
    private final TopicDefinitionFactory topicDefinitionFactory = new TopicDefinitionFactory(mockStroomPropertyService);

    @Mock
    private StatisticsService mockStatisticsService;

    @Mock
    private ConsumerFactory consumerFactory;

    private final Producer<StatEventKey, StatAggregate> mockProducer = new MockProducer<>();
    private final Consumer<StatEventKey, StatAggregate> mockConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() {

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        StatisticsAggregationProcessor statisticsAggregationProcessor = new StatisticsAggregationProcessor(
                topicDefinitionFactory,
                mockStatisticsService,
                mockStroomPropertyService,
                StatisticType.COUNT,
                EventStoreTimeIntervalEnum.MINUTE,
                mockProducer,
                executorService,
                consumerFactory,
                1);



    }

    private static class MockConsumerFactory implements ConsumerFactory {

        private final Consumer<StatEventKey, StatAggregate> consumer;

        private MockConsumerFactory(final Consumer<StatEventKey, StatAggregate> consumer) {
            this.consumer = consumer;
        }

        @Override
        public <K, V> Consumer<K, V> createConsumer(final String groupId,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde) {
            return null;
        }
    }
}