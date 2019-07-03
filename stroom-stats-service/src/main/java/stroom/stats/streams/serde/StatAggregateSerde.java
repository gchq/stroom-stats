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

package stroom.stats.streams.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.util.logging.LambdaLogger;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StatAggregateSerde implements Serde<StatAggregate>,
        Serializer<StatAggregate>,
        Deserializer<StatAggregate> {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatAggregateSerde.class);

    private static final KryoFactory factory = () -> {
        Kryo kryo = new Kryo();
        try {
            LOGGER.debug(() -> String.format("Initialising Kryo on thread %s",
                    Thread.currentThread().getName()));

            kryo.register(ValueAggregate.class, 11);
            kryo.register(CountAggregate.class, 12);
            kryo.register(StatAggregate.class, 13);
            kryo.register(MultiPartIdentifier.class, 14);
            kryo.register(List.class, 15);
            kryo.register(ArrayList.class, 16);
            kryo.register(Collections.EMPTY_LIST.getClass(), 17);
            kryo.register(byte[].class, 18);
            kryo.register(Object[].class, 19);
            kryo.register(Double.class, 20);
            kryo.register(Integer.class, 21);

            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(
                    new StdInstantiatorStrategy());
            kryo.setRegistrationRequired(true);
        } catch (Exception e) {
            LOGGER.error("Exception occurred configuring kryo instance", e);
        }
        return kryo;
    };

    private static final KryoPool pool = new KryoPool.Builder(factory)
            .softReferences()
            .build();

    private StatAggregateSerde() {
    }

    public static StatAggregateSerde instance() {
        return new StatAggregateSerde();
    }

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //no configuration needed
    }

    /**
     * @param topic          topic associated with data
     * @param statAggregate typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, StatAggregate statAggregate) {
        return pool.run(kryo -> {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Output output = new Output(stream);
            kryo.writeClassAndObject(output, statAggregate);
            output.close();
            return stream.toByteArray();
        });
    }

    /**
     * Deserialize a record value from a bytearray into a value or object.
     *
     * @param topic topic associated with the data
     * @param bytes serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public StatAggregate deserialize(String topic, byte[] bytes) {
        return pool.run(kryo -> {
            Input input = new Input(bytes);
            return (StatAggregate) kryo.readClassAndObject(input);
        });
    }

    /**
     * Close this serializer.
     * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
     * multiple times.
     */
    @Override
    public void close() {
        //nothing to close
    }

    @Override
    public Serializer<StatAggregate> serializer() {
        return this;
    }

    @Override
    public Deserializer<StatAggregate> deserializer() {
        return this;
    }
}
