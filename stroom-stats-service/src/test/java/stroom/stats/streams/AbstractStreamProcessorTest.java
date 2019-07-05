package stroom.stats.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.streams.topics.TopicDefinition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;

abstract class AbstractStreamProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamProcessorTest.class);

    <K,V> ConsumerRecordFactory<K,V> getConsumerRecordFactory(final TopicDefinition<K, V> topicDefinition) {
        return new ConsumerRecordFactory<>(
                topicDefinition.getName(),
                topicDefinition.getKeySerde().serializer(),
                topicDefinition.getValueSerde().serializer());
    }

    <K,V> Optional<ProducerRecord<K,V>> readProducerRecord(final TopicDefinition<K,V> topicDefinition,
                                                           final TopologyTestDriver testDriver) {

        return Optional.ofNullable(testDriver.readOutput(
                topicDefinition.getName(),
                topicDefinition.getKeySerde().deserializer(),
                topicDefinition.getValueSerde().deserializer()));
    }

    <K,V> List<ProducerRecord<K,V>> readAllProducerRecords(final TopicDefinition<K, V> topicDefinition,
                                                           final TopologyTestDriver testDriver) {

        ProducerRecord<K,V> producerRecord = null;
        List<ProducerRecord<K,V>> producerRecords = new ArrayList<>();

        do {
            producerRecord = testDriver.readOutput(
                    topicDefinition.getName(),
                    topicDefinition.getKeySerde().deserializer(),
                    topicDefinition.getValueSerde().deserializer());

            if (producerRecord != null) {
                producerRecords.add(producerRecord);
            }
        } while (producerRecord != null);
        return producerRecords;
    }

    /**
     * Reads all messages on the supplied topics into a map, assuming all topics have the same
     * KV types.
     */
    <K,V> Map<TopicDefinition<K,V>, List<ProducerRecord<K,V>>> readAllProducerRecords(
            final Collection<TopicDefinition<K, V>> topicDefinitions,
            final TopologyTestDriver testDriver) {

        final Map<TopicDefinition<K,V>, List<ProducerRecord<K,V>>> map = new HashMap<>();

        topicDefinitions.forEach(topicDefinition -> {
            List<ProducerRecord<K,V>> records = readAllProducerRecords(topicDefinition, testDriver);
            map.put(topicDefinition, records);
        });
        return map;
    }

    <K,V> void sendMessage(final TopologyTestDriver testDriver,
                           final ConsumerRecordFactory<K,V> consumerRecordFactory,
                           final K key,
                           final V value) {

        LOGGER.debug("Sending key: {}, value: {}", key, value);
        ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordFactory.create(
                key,
                value);

        testDriver.pipeInput(consumerRecord);
    }

    <K,V> void sendMessages(final TopologyTestDriver testDriver,
                           final ConsumerRecordFactory<K,V> consumerRecordFactory,
                           final List<KeyValue<K,V>> keyValues) {

        keyValues.forEach(keyValue -> {
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordFactory.create(
                    keyValue.key,
                    keyValue.value);

            testDriver.pipeInput(consumerRecord);
        });
    }

    <K,V> KeyValue<K,V> convertKeyValue(final TopicDefinition<K,V> topicDefinition,
                                        final byte[] keyBytes,
                                        byte[] valueBytes) {
        K key = keyBytes == null ? null : topicDefinition.getKeySerde()
                .deserializer()
                .deserialize(topicDefinition.getName(), keyBytes);

        V value = valueBytes == null ? null : topicDefinition.getValueSerde()
                .deserializer()
                .deserialize(topicDefinition.getName(), valueBytes);

        return new KeyValue<>(key, value);
    }

    protected Properties getStreamConfigProperties() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "not_used_but_required:9999");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        props.put("cache.max.bytes.buffering", 1024L);

        final String user = Optional.ofNullable(System.getProperty("user.name")).orElse("unknownUser");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-" + user);
        return props;
    }


    <K,V> void runProcessorTest(final TopicDefinition<K,V> inputTopicDefinition,
                                final StreamProcessor streamProcessor,
                                final BiConsumer<TopologyTestDriver, ConsumerRecordFactory<K,V>> testAction) {

        deleteStreamsStateDir(streamProcessor);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(
                streamProcessor.getTopology(),
                streamProcessor.getStreamConfig())) {

            ConsumerRecordFactory<K,V> factory = getConsumerRecordFactory(inputTopicDefinition);

            // perform the test
            testAction.accept(testDriver, factory);
        }
    }

    private void deleteStreamsStateDir(final StreamProcessor streamProcessor) {

        final String stateDirPathStr = streamProcessor.getStreamConfig().getProperty(StreamsConfig.STATE_DIR_CONFIG);
        if (stateDirPathStr != null) {
            if (!stateDirPathStr.startsWith("/tmp")) {
                // as we are doing a recursive delete add a bit of protection to ensure we don't accidentally wipe our
                // filesystem
                throw new RuntimeException("Expecting state dir to be somewhere in /tmp");
            }
            final Path stateDir = Paths.get(stateDirPathStr);

            if (Files.isDirectory(stateDir)) {
                LOGGER.info("Deleting kafka-streams state directory {} and its contents", stateDir);
                // Clear out the state dir
                try {
                    Files.walk(stateDir)
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to delete state dir " + stateDir, e);
                }
            }
        } else {
            LOGGER.info("Property {} not defined so won't delete any state", StreamsConfig.STATE_DIR_CONFIG);
        }
    }
}
