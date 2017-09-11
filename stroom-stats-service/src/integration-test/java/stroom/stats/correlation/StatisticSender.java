package stroom.stats.correlation;

import com.google.inject.Injector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import stroom.stats.api.StatisticType;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.v3.Statistics;
import stroom.stats.streams.FullEndToEndIT;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.streams.TopicNameFactory;
import stroom.stats.util.logging.LambdaLogger;
import stroom.stats.xml.StatisticsMarshaller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class StatisticSender {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(FullEndToEndIT.class);

    public static void sendStatistics(Injector injector, Statistics statistics, StatisticType statisticType) throws InterruptedException {
        StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
        StatisticsMarshaller statisticsMarshaller = injector.getInstance(StatisticsMarshaller.class);
        String topicPrefix = stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX);
        String topic = TopicNameFactory.getStatisticTypedName(topicPrefix, statisticType);
        StatisticSender.sendStatistics(
                StatisticSender.buildKafkaProducer(stroomPropertyService),
                topic,
                Arrays.asList(statistics),
                statisticsMarshaller);
    }

    private static void sendStatistics(
            KafkaProducer<String, String> kafkaProducer,
            String topic,
            List<Statistics> statisticsList,
            StatisticsMarshaller statisticsMarshaller){

        statisticsList.forEach(
            statistics -> {
                ProducerRecord<String, String> producerRecord = buildProducerRecord(topic, statistics, statisticsMarshaller);

                statistics.getStatistic().forEach(statistic ->
                        LOGGER.trace("Sending stat with uuid {}, name {}, count {} and value {}",
                                statistic.getKey().getValue(),
                                statistic.getKey().getStatisticName(),
                                statistic.getCount(),
                                statistic.getValue())
                );

                LOGGER.trace(() -> String.format("Sending %s stat events to topic %s", statistics.getStatistic().size(), topic));
                try {
                    kafkaProducer.send(producerRecord).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw  new RuntimeException("Interrupted", e);
                } catch (ExecutionException e) {
                    throw  new RuntimeException("Error sending record to Kafka", e);
                }
            });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private static KafkaProducer<String, String> buildKafkaProducer(StroomPropertyService stroomPropertyService){
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

    private static ProducerRecord<String, String> buildProducerRecord(String topic, Statistics statistics, StatisticsMarshaller statisticsMarshaller) {
        String statKey = statistics.getStatistic().get(0).getKey().getValue();
        return new ProducerRecord<>(topic, statKey, statisticsMarshaller.marshallToXml(statistics));
    }
}
