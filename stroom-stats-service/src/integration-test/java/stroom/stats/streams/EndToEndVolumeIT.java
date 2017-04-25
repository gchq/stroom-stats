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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.Assertions;
import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.SearchRequest;
import stroom.query.api.SearchResponse;
import stroom.stats.AbstractAppIT;
import stroom.stats.HBaseClient;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.QueryApiHelper;
import stroom.stats.test.StatisticConfigurationEntityHelper;
import stroom.stats.xml.StatisticsMarshaller;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class EndToEndVolumeIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(EndToEndVolumeIT.class);

    public static final String STATISTIC_EVENTS_TOPIC_PREFIX = "statisticEvents";
    public static final String BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "badStatisticEvents";
    private static final Map<StatisticType, String> INPUT_TOPICS_MAP = new HashMap<>();
    private static final Map<StatisticType, String> BAD_TOPICS_MAP = new HashMap<>();

//    private static final int ITERATION_COUNT = 52_000;
    private static final int ITERATION_COUNT = 1_000;
    //5_000 is about 17hrs at 5000ms intervals

    public static final EventStoreTimeIntervalEnum SMALLEST_INTERVAL = EventStoreTimeIntervalEnum.SECOND;

    private Injector injector = getApp().getInjector();
    private StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
    private HBaseClient hBaseClient = injector.getInstance(HBaseClient.class);

    @Before
    public void setup() {

        Arrays.stream(StatisticType.values())
                .forEachOrdered(type -> {
                    String inputTopic = TopicNameFactory.getStatisticTypedName(STATISTIC_EVENTS_TOPIC_PREFIX, type);
                    INPUT_TOPICS_MAP.put(type, inputTopic);

                    String badTopic = TopicNameFactory.getStatisticTypedName(BAD_STATISTIC_EVENTS_TOPIC_PREFIX, type);
                    BAD_TOPICS_MAP.put(type, badTopic);
                });

        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 5_000);
        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 500);
        //setting this to latest ensure we don't pick up messages from previous runs, requires us to spin up the processors
        //before putting msgs on the topics
        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_AUTO_OFFSET_RESET, "latest");
    }

    @Test
    public void volumeTest_count() throws InterruptedException {

        final StatisticType statisticType = StatisticType.COUNT;


        final int eventsPerIteration = GenerateSampleStatisticsData.COLOURS.size() *
                GenerateSampleStatisticsData.USERS.length *
                GenerateSampleStatisticsData.STATES.size();

        int expectedTotalEvents = ITERATION_COUNT * eventsPerIteration;

        LOGGER.info("Expecting {} events per stat type", expectedTotalEvents);

        int expectedTotalCountCount = (int) (GenerateSampleStatisticsData.COUNT_STAT_VALUE * expectedTotalEvents);

        //wait for a bit to give the consumers a chance to spin up
        Thread.sleep(1_000);

        LOGGER.info("Starting to load data (async)...");
        //create stat configs and put the test data on the topics
        Map<StatisticType, StatisticConfigurationEntity> statConfigs = loadData(statisticType);
        LOGGER.info("Finished loading data (async)...");

        StatisticConfiguration statisticConfiguration = statConfigs.get(statisticType);

        //run a query that will use the zero mask
        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            SearchRequest searchRequest = QueryApiHelper.buildSearchRequestAllDataAllFields(
                    statisticConfiguration,
                    interval);

            repeatedQueryAndAssert(searchRequest,
                    interval,
                    getRowCountsByInterval(eventsPerIteration).get(interval),
                    expectedTotalEvents,
                    rowData -> {
                        Assertions.assertThat(getCountFieldSum(rowData)
                        ).isEqualTo(expectedTotalCountCount);
                    });
        }

        //now run queries that will roll all tags up
        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            SearchRequest searchRequest = QueryApiHelper.buildSearchRequestAllData(
                    statisticConfiguration,
                    interval,
                    Arrays.asList(
                            StatisticConfiguration.FIELD_NAME_STATISTIC,
                            StatisticConfiguration.FIELD_NAME_DATE_TIME,
                            StatisticConfiguration.FIELD_NAME_PRECISION,
                            StatisticConfiguration.FIELD_NAME_COUNT
                    ));

            repeatedQueryAndAssert(searchRequest,
                    interval,
                    getRowCountsByInterval(1).get(interval),
                    expectedTotalEvents,
                    rowData ->
                            Assertions.assertThat(
                                    getCountFieldSum(rowData)
                            ).isEqualTo(expectedTotalCountCount));
        }
    }

    private long getCountFieldSum(final List<Map<String, String>> rowData) {
        return rowData.stream()
                .map(rowMap -> rowMap.get(StatisticConfiguration.FIELD_NAME_COUNT.toLowerCase()))
                .mapToLong(Long::valueOf)
                .sum();
    }

    @Test
    public void volumeTest_value() throws InterruptedException {

        final StatisticType statisticType = StatisticType.VALUE;


        final int eventsPerIteration = GenerateSampleStatisticsData.COLOURS.size() *
                GenerateSampleStatisticsData.USERS.length *
                GenerateSampleStatisticsData.STATES.size();

        int expectedTotalEvents = ITERATION_COUNT * eventsPerIteration;

        LOGGER.info("Expecting {} events per stat type", expectedTotalEvents);

        double expectedTotalValueCount = GenerateSampleStatisticsData.VALUE_STAT_VALUE_MAP.values().stream()
                .mapToDouble(Double::doubleValue)
                .sum() * expectedTotalEvents;

        //wait for a bit to give the consumers a chance to spin up
        Thread.sleep(1_000);

        //create stat configs and put the test data on the topics
        Map<StatisticType, StatisticConfigurationEntity> statConfigs = loadData(statisticType);

        StatisticConfiguration statisticConfiguration = statConfigs.get(statisticType);

        //run a query that will use the zero mask
        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            SearchRequest searchRequest = QueryApiHelper.buildSearchRequestAllDataAllFields(
                    statisticConfiguration,
                    interval);

            repeatedQueryAndAssert(searchRequest,
                    interval,
                    getRowCountsByInterval(eventsPerIteration).get(interval),
                    expectedTotalEvents,
                    rowData -> {
                        Assertions.assertThat(getCountFieldSum(rowData)).isEqualTo(expectedTotalEvents);
                    });
        }

        //now run queries that will roll all tags up
        for (EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            SearchRequest searchRequest = QueryApiHelper.buildSearchRequestAllData(
                    statisticConfiguration,
                    interval,
                    Arrays.asList(
                            StatisticConfiguration.FIELD_NAME_DATE_TIME,
                            StatisticConfiguration.FIELD_NAME_PRECISION,
                            StatisticConfiguration.FIELD_NAME_COUNT
                    ));

            repeatedQueryAndAssert(searchRequest,
                    interval,
                    getRowCountsByInterval(1).get(interval),
                    expectedTotalEvents,
                    rowData ->
                            Assertions.assertThat(getCountFieldSum(rowData)).isEqualTo(expectedTotalEvents));
        }
    }

    private void repeatedQueryAndAssert(SearchRequest searchRequest,
                                        EventStoreTimeIntervalEnum interval,
                                        int expectedRowCount,
                                        int expectedTotalEvents,
                                        Consumer<List<Map<String, String>>> rowDataConsumer) throws InterruptedException {

        List<Map<String, String>> rowData = Collections.emptyList();
        Instant timeoutTime = Instant.now().plus(4, ChronoUnit.MINUTES);

        //query the store repeatedly until we get the answer we want or give up
        while ((rowData.size() != expectedRowCount || getCountFieldSum(rowData) != expectedTotalEvents) &&
                Instant.now().isBefore(timeoutTime)) {

            Thread.sleep(1_000);

            rowData = runSearch(searchRequest);

            LOGGER.info("{} store returned row count: {}, waiting for {} rows and a sum of the count field of {}",
                    interval, rowData.size(), expectedRowCount, expectedTotalEvents);

        }

        Assertions.assertThat(rowData).hasSize(expectedRowCount);

        LOGGER.debug("Distinct values: {}", rowData.stream()
                .map(rowMap -> rowMap.get(StatisticConfiguration.FIELD_NAME_COUNT.toLowerCase()))
                .distinct()
                .collect(Collectors.joining(",")));

        dumpRowData(rowData, 100);

        if (rowDataConsumer != null) {
            rowDataConsumer.accept(rowData);
        }
    }

    private void dumpRowData(List<Map<String, String>> rowData, @Nullable Integer maxRows) {
        Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put(StatisticConfiguration.FIELD_NAME_DATE_TIME.toLowerCase(), Instant.class);

        List<Map<String, String>> rows = maxRows == null ? rowData : rowData.subList(0, Math.min(maxRows, rowData.size()));

        String tableStr = QueryApiHelper.convertToFixedWidth(rows, typeMap).stream()
                .collect(Collectors.joining("\n"));
        if (maxRows != null) {
            tableStr += "\n...TRUNCATED...";
        }
        LOGGER.info("Dumping row data:\n" + tableStr);

    }

    private Map<EventStoreTimeIntervalEnum, Integer> getRowCountsByInterval(int eventsPerIteration) {
        Map<EventStoreTimeIntervalEnum, Integer> countsMap = new HashMap<>();

        int timeSpanMs = ITERATION_COUNT * GenerateSampleStatisticsData.EVENT_TIME_DELTA_MS;
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            int intervalCount;
            if (interval.columnInterval() < GenerateSampleStatisticsData.EVENT_TIME_DELTA_MS) {
                double fractionalIntervalCount = (double) timeSpanMs / GenerateSampleStatisticsData.EVENT_TIME_DELTA_MS;
                intervalCount = (int) Math.ceil(fractionalIntervalCount);
            } else {
                double fractionalIntervalCount = (double) timeSpanMs / interval.columnInterval();
                intervalCount = (int) Math.ceil(fractionalIntervalCount);
            }
            countsMap.put(interval, intervalCount * eventsPerIteration);
        }
        return countsMap;
    }

    private List<Map<String, String>> runSearch(final SearchRequest searchRequest) {

        SearchResponse searchResponse = hBaseClient.query(searchRequest);

        return QueryApiHelper.getRowData(searchRequest, searchResponse);
    }

    private Map<StatisticType, StatisticConfigurationEntity> loadData(StatisticType... statisticTypes) {

        final KafkaProducer<String, String> kafkaProducer = buildKafkaProducer(stroomPropertyService);

//        StatisticType[] types = new StatisticType[] {StatisticType.COUNT};

        Map<StatisticType, StatisticConfigurationEntity> statConfigs = new HashMap<>();

        int numberOfStatWrappers = Math.max(1, ITERATION_COUNT / 250);

        for (StatisticType statisticType : statisticTypes) {

            String statNameStr = "VolumeTest-" + Instant.now().toString() + "-" + statisticType + "-" + SMALLEST_INTERVAL;



            Tuple2<StatisticConfigurationEntity, List<Statistics>> testData = GenerateSampleStatisticsData.generateData(
                    statNameStr,
                    statisticType,
                    SMALLEST_INTERVAL,
                    StatisticRollUpType.ALL,
                    numberOfStatWrappers,
                    ITERATION_COUNT);

            persistStatConfig(testData._1());
            statConfigs.put(statisticType, testData._1());

            String inputTopic = INPUT_TOPICS_MAP.get(statisticType);
            List<Statistics> testEvents = testData._2();

            LOGGER.info("Sending {} statistics wrappers of type {} to topic {}", testEvents.size(), statisticType, inputTopic);
            testEvents.parallelStream().forEach(statisticsObj -> {
                ProducerRecord<String, String> producerRecord = buildProducerRecord(
                        inputTopic,
                        statisticsObj,
                        injector.getInstance(StatisticsMarshaller.class));

                try {
                    kafkaProducer.send(producerRecord).get();
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
        return statConfigs;
    }


//    private void setNumStreamThreads(final int newValue) {
//        stroomPropertyService.setProperty(
//                StatisticsIngestService.PROP_KEY_KAFKA_STREAM_THREADS, newValue);
//    }
//
//    private static void configure(StroomPropertyService stroomPropertyService) {
//        //make sure the purge retention periods are very large so no stats get purged
//        Arrays.stream(EventStoreTimeIntervalEnum.values()).forEach(interval ->
//                stroomPropertyService.setProperty(
//                        HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX +
//                                interval.name().toLowerCase(), 10_000));
//
//        //set small batch size and flush interval so we don't have to wait ages for data to come through
//
//        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MIN_BATCH_SIZE, 10);
//        stroomPropertyService.setProperty(StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 500);
//
//        stroomPropertyService.setProperty(StatisticsIngestService.PROP_KEY_KAFKA_STREAM_THREADS, 1);
//    }

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
        consumerProps.put("bootstrap.servers", stroomPropertyService.getProperty(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS).get());
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

    private void persistStatConfig(final StatisticConfigurationEntity statisticConfigurationEntity) {
        StatisticConfigurationEntityHelper.addStatConfig(
                injector.getInstance(SessionFactory.class),
                injector.getInstance(StatisticConfigurationEntityMarshaller.class),
                statisticConfigurationEntity);
    }


}
