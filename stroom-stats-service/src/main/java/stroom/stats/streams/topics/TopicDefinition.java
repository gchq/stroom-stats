package stroom.stats.streams.topics;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Objects;

public class TopicDefinition<K,V> {
    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public TopicDefinition(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.name = Objects.requireNonNull(name);
        this.keySerde = Objects.requireNonNull(keySerde);
        this.valueSerde = Objects.requireNonNull(valueSerde);
    }

    public static TopicDefinition<String, String> createStringStringTopic(final String name) {
        return new TopicDefinition<>(name, Serdes.String(), Serdes.String());
    }

    public static <K, V> TopicDefinition<K, V> createTopic(
            final String name,
            final Serde<K> keySerde,
            final Serde<V> valueSerde) {

        return new TopicDefinition<>(name, keySerde, valueSerde);
    }

    public String getName() {
        return name;
    }

    public Serde<K> getKeySerde() {
        return keySerde;
    }

    public Serde<V> getValueSerde() {
        return valueSerde;
    }


    public Consumed<K,V> getConsumed() {
        return Consumed.with(keySerde, valueSerde);
    }


    public Produced<K,V> getProduced() {
        return Produced.with(keySerde, valueSerde);
    }

    public Produced<K,V> getProduced(final StreamPartitioner<? super K, ? super V> partitioner) {
        return Produced.with(keySerde, valueSerde, partitioner);
    }

    public Serialized<K,V> getSerialized() {
        return Serialized.with(keySerde, valueSerde);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TopicDefinition<?, ?> that = (TopicDefinition<?, ?>) o;
        return name.equals(that.name) &&
                keySerde.equals(that.keySerde) &&
                valueSerde.equals(that.valueSerde);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, keySerde, valueSerde);
    }

    @Override
    public String toString() {
        return "TopicDefinition{" +
                "name='" + name + '\'' +
                '}';
    }
}
