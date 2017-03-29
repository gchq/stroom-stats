package stroom.stats.testdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.streams.FullEndToEndIT;
import stroom.stats.streams.KafkaStreamService;
import stroom.stats.util.logging.LambdaLogger;
import stroom.stats.xml.StatisticsMarshaller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaHelper {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(FullEndToEndIT.class);

    public static void sendDummyStatistics(
            KafkaProducer<String, String> kafkaProducer,
            String topic,
            List<Statistics> statisticsList,
            StatisticsMarshaller statisticsMarshaller){



        statisticsList.stream().forEach(
                        statistics -> {
                            ProducerRecord<String, String> producerRecord = buildProducerRecord(topic, statistics, statisticsMarshaller);

                            statistics.getStatistic().forEach(statistic ->
                                    LOGGER.trace("Sending stat with name {}, count {} and value {}", statistic.getName(), statistic.getCount(), statistic.getValue())
                            );

                            LOGGER.trace(() -> String.format("Sending %s stat events to topic $s", statistics.getStatistic().size(), topic));
                            kafkaProducer.send(producerRecord);
                        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

    public static KafkaProducer<String, String> buildKafkaProducer(StroomPropertyService stroomPropertyService){
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                stroomPropertyService.getPropertyOrThrow(KafkaStreamService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5_000_000);

        Serde<String> stringSerde = Serdes.String();

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps, stringSerde.serializer(), stringSerde.serializer());
        return kafkaProducer;
    }

    private static ProducerRecord<String, String> buildProducerRecord(String topic, Statistics statistics, StatisticsMarshaller statisticsMarshaller) {
        String statName = statistics.getStatistic().get(0).getName();
        return new ProducerRecord<>(topic, statName, statisticsMarshaller.marshallXml(statistics));
    }
}
