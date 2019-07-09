package stroom.stats.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Serde;

public interface ConsumerFactory {

    <K, V> Consumer<K, V> createConsumer(final String groupId,
                                         final Serde<K> keySerde,
                                         final Serde<V> valueSerde);
}
