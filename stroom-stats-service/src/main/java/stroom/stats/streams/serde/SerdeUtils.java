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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.util.logging.LambdaLogger;

import java.util.Map;

public class SerdeUtils {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(SerdeUtils.class);

    @FunctionalInterface
    public interface SerializeFunc<T> {
        byte[] serialize(final String topic, final T obj);
    }

    @FunctionalInterface
    public interface DeserializeFunc<T> {
        T deserialize(final String topic, final byte[] bytes);
    }

    /**
     * Creates a basic serializer using the passed stateless function and not implementing close or configure
     *
     * @param serializeFunc The function to serialize T to a byte[]
     * @param <T>           The type of object to serialize
     * @return A byte[] representation of T
     */
    public static <T> Serializer<T> buildBasicSerializer(final SerializeFunc<T> serializeFunc) {
        return new Serializer<T>() {

            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
            }

            @Override
            public byte[] serialize(final String topic, final T data) {
                return serializeFunc.serialize(topic, data);
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Builds a Deserializer of T with the passed stateless function and no configure or close implementations
     */
    public static <T> Deserializer<T> buildBasicDeserializer(final DeserializeFunc<T> deserializeFunc) {
        return new Deserializer<T>() {
            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
            }

            @Override
            public T deserialize(final String topic, final byte[] bData) {
                return deserializeFunc.deserialize(topic, bData);
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Builds a Serde for T using a basic Serializer and Deserializer that do not implement configure or close
     */
    public static <T> Serde<T> buildBasicSerde(final SerializeFunc<T> serializeFunc, final DeserializeFunc<T> deserializeFunc) {
        return Serdes.serdeFrom(buildBasicSerializer(serializeFunc), buildBasicDeserializer(deserializeFunc));
    }

    /**
     * Helper method for testing that a serde can serialize its object and deserialize it
     * back again. Throws a {@link RuntimeException } if the objects don't match.
     * @param serde
     * @param obj
     * @param <T>
     */
    static <T> void verify(final Serde<T> serde, final T obj) {
        final String dummyTopic = "xxx";
        byte[] bytes = serde.serializer().serialize(dummyTopic, obj);

        LOGGER.trace(() -> String.format("object form: %s", obj));
        LOGGER.trace(() -> String.format("byte form: %s", ByteArrayUtils.byteArrayToHex(bytes)));

        T deserializedObj = serde.deserializer().deserialize(dummyTopic, bytes);
        if (!obj.equals(deserializedObj)) {
            throw new RuntimeException(String.format("Original [%s] and de-serialized [%s] values don't match", obj, deserializedObj));
        }
    }
}
