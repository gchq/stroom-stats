package stroom.stats.correlation;

import com.google.inject.Injector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import stroom.stats.api.StatisticType;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.streams.FullEndToEndIT;
import stroom.stats.streams.KafkaStreamService;
import stroom.stats.streams.TopicNameFactory;
import stroom.stats.util.logging.LambdaLogger;
import stroom.stats.xml.StatisticsMarshaller;
import stroom.util.thread.ThreadUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatisticSender {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(FullEndToEndIT.class);

    public static void sendStatistics(Injector injector, Statistics statistics, StatisticType statisticType){
        StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
        StatisticsMarshaller statisticsMarshaller = injector.getInstance(StatisticsMarshaller.class);
        String topicPrefix = stroomPropertyService.getPropertyOrThrow(KafkaStreamService.PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX);
        String topic = TopicNameFactory.getStatisticTypedName(topicPrefix, statisticType);
        StatisticSender.sendStatistics(
                StatisticSender.buildKafkaProducer(stroomPropertyService),
                topic,
                Arrays.asList(statistics),
                statisticsMarshaller);
        // Waiting for a bit so that we know the Statistics have been processed
        ThreadUtil.sleep(60_000);
        //TODO it'd be useful to check HBase and see if the stats have been created
    }

    private static void sendStatistics(
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

                LOGGER.trace(() -> String.format("Sending %s stat events to topic %s", statistics.getStatistic().size(), topic));
                kafkaProducer.send(producerRecord);
            });

        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private static KafkaProducer<String, String> buildKafkaProducer(StroomPropertyService stroomPropertyService){
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
