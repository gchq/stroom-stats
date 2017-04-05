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

package stroom.stats.streams;

import com.google.inject.Injector;
import javaslang.Tuple2;
import javaslang.Tuple3;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticConfigurationEntityHelper;
import stroom.stats.test.StatisticsHelper;
import stroom.stats.xml.StatisticsMarshaller;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class EndToEndVolumeTestIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(EndToEndVolumeTestIT.class);

    public static final String STATISTIC_EVENTS_TOPIC_PREFIX = "statisticEvents";
    public static final String BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "badStatisticEvents";
    private static final Map<StatisticType, String> INPUT_TOPICS_MAP = new HashMap<>();
    private static final Map<StatisticType, String> BAD_TOPICS_MAP = new HashMap<>();

    public static final EventStoreTimeIntervalEnum SMALLEST_INTERVAL = EventStoreTimeIntervalEnum.SECOND;

    private Injector injector = getApp().getInjector();
    private StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);

    @Before
    public void setup() {

        Arrays.stream(StatisticType.values())
                .forEachOrdered(type -> {
                    String inputTopic = TopicNameFactory.getStatisticTypedName(STATISTIC_EVENTS_TOPIC_PREFIX, type);
                    INPUT_TOPICS_MAP.put(type, inputTopic);

                    String badTopic = TopicNameFactory.getStatisticTypedName(BAD_STATISTIC_EVENTS_TOPIC_PREFIX, type);
                    BAD_TOPICS_MAP.put(type, badTopic);
                });
    }

    @Test
    public void volumeTest() {

    }

    private void loadData() {

        final KafkaProducer<String, String> kafkaProducer = buildKafkaProducer(stroomPropertyService);

//        StatisticType[] types = new StatisticType[] {StatisticType.COUNT};
        StatisticType[] types = StatisticType.values();

        for (StatisticType statisticType : types) {

            String statNameStr = "VolumeTest-" + Instant.now().toString() + "-" + statisticType + "-" + SMALLEST_INTERVAL;

            Tuple2<StatisticConfigurationEntity, List<Statistics>> testData = GenerateSampleStatisticsData.generateData(
                    statNameStr,
                    statisticType,
                    SMALLEST_INTERVAL,
                    StatisticRollUpType.ALL,
                    500);

            persistStatConfig(testData._1());

            String inputTopic = INPUT_TOPICS_MAP.get(statisticType);
            testData._2().forEach(statisticsObj -> {
                ProducerRecord<String, String> producerRecord = buildProducerRecord(
                        inputTopic,
                        statisticsObj,
                        injector.getInstance(StatisticsMarshaller.class));

                try {
                    kafkaProducer.send(producerRecord).get()
                } catch (InterruptedException e) {
                    //just used in testing so bomb out
                    throw new RuntimeException(String.format("Thread interrupted"), e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(String.format("Error sending record to kafka topic %s", inputTopic), e);
                }
            });
        }

        Map<String, List<String>> badEvents = new HashMap<>();
        startBadEventsConsumer(badEvents);

    }


    private void setNumStreamThreads(final int newValue) {
        stroomPropertyService.setProperty(
                KafkaStreamService.PROP_KEY_KAFKA_STREAM_THREADS, newValue);
    }

    private static void configure(StroomPropertyService stroomPropertyService) {
        //make sure the purge retention periods are very large so no stats get purged
        Arrays.stream(EventStoreTimeIntervalEnum.values()).forEach(interval ->
                stroomPropertyService.setProperty(
                        HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX +
                                interval.name().toLowerCase(), 10_000));

        //set small batch size and flush interval so we don't have to wait ages for data to come through

        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10);
        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 500);

        stroomPropertyService.setProperty(KafkaStreamService.PROP_KEY_KAFKA_STREAM_THREADS, 1);
    }

    private static KafkaProducer<String, String> buildKafkaProducer(StroomPropertyService stroomPropertyService) {
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

    /**
     * Start a consume consuming from both bad events topics, log each message and add each message
     * into a map keyed by topic name
     * A {@link CountDownLatch} is returned to allow the caller to wait for the expected number of messages
     */
    private void startBadEventsConsumer(Map<String, List<String>> messages) {

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", stroomPropertyService.getProperty(KafkaStreamService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS).get());
        consumerProps.put("group.id", this.getClass().getName() + "-groupId");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());

            //Subscribe to all bad event topics
            kafkaConsumer.subscribe(BAD_TOPICS_MAP.values());

            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        LOGGER.warn("Bad events Consumer - topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        messages.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record.value());
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });
    }

    private void addStatConfig(String statName,
                               StatisticType statisticType,
                               EventStoreTimeIntervalEnum interval,
                               String... fieldNames) {

        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                statName,
                statisticType,
                interval.columnInterval(),
                StatisticRollUpType.ALL)
                .addFields(fieldNames)
                .build();

        persistStatConfig(statisticConfigurationEntity);
    }

    private void persistStatConfig(final StatisticConfigurationEntity statisticConfigurationEntity) {
        StatisticConfigurationEntityHelper.addStatConfig(
                injector.getInstance(SessionFactory.class),
                injector.getInstance(StatisticConfigurationEntityMarshaller.class),
                statisticConfigurationEntity);
    }

}
