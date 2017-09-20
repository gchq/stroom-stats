package stroom.stats.correlation;

import com.google.inject.Injector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import stroom.stats.api.StatisticType;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.v4.Statistics;
import stroom.stats.schema.v4.StatisticsMarshaller;
import stroom.stats.streams.FullEndToEndIT;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.streams.TopicNameFactory;
import stroom.stats.test.StatMessage;
import stroom.stats.util.logging.LambdaLogger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class StatisticSender {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(FullEndToEndIT.class);

    public static void sendStatistics(final Injector injector,
                                      final String statUuid,
                                      final Statistics statistics,
                                      final StatisticType statisticType) throws InterruptedException {

        StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
        StatisticsMarshaller statisticsMarshaller = injector.getInstance(StatisticsMarshaller.class);
        String topicPrefix = stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX);
        String topic = TopicNameFactory.getStatisticTypedName(topicPrefix, statisticType);
        StatMessage statMessage = new StatMessage(topic, statUuid, statistics);
        StatisticSender.sendStatistics(
                StatisticSender.buildKafkaProducer(stroomPropertyService),
                Arrays.asList(statMessage),
                statisticsMarshaller);
    }

    private static void sendStatistics(final KafkaProducer<String, String> kafkaProducer,
                                       final List<StatMessage> statMessages,
                                       final StatisticsMarshaller statisticsMarshaller) {

        statMessages.forEach(
                statMsg -> {
                    ProducerRecord<String, String> producerRecord = buildProducerRecord(
                            statMsg.getTopic(),
                            statMsg.getKey(),
                            statMsg.getStatistics(),
                            statisticsMarshaller);

                    statMsg.getStatistics().getStatistic().forEach(statistic ->
                            LOGGER.trace("Sending stat with uuid {}, name {}, count {} and value {}",
                                    statMsg.getKey(),
                                    statistic.getCount(),
                                    statistic.getValue())
                    );

                    LOGGER.trace(() -> String.format("Sending %s stat events to topic %s",
                            statMsg.getStatistics().getStatistic().size(), statMsg.getTopic()));
                    try {
                        kafkaProducer.send(producerRecord).get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted", e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException("Error sending record to Kafka", e);
                    }
                });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private static KafkaProducer<String, String> buildKafkaProducer(StroomPropertyService stroomPropertyService) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5_000_000);

        Serde<String> stringSerde = Serdes.String();

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps, stringSerde.serializer(), stringSerde.serializer());
        return kafkaProducer;
    }

    private static ProducerRecord<String, String> buildProducerRecord(final String topic,
                                                                      final String key,
                                                                      final Statistics statistics,
                                                                      final StatisticsMarshaller statisticsMarshaller) {
        return new ProducerRecord<>(topic, key, statisticsMarshaller.marshallToXml(statistics));
    }
}
