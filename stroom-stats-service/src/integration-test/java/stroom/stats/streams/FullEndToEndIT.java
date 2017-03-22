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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.hibernate.SessionFactory;
import org.junit.Test;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.schema.TagType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticConfigurationEntityHelper;
import stroom.stats.test.StatisticsHelper;
import stroom.stats.util.logging.LambdaLogger;
import stroom.stats.xml.StatisticsMarshaller;
import stroom.util.thread.ThreadUtil;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FullEndToEndIT extends AbstractAppIT {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(FullEndToEndIT.class);

    private Injector injector = getApp().getInjector();
    private SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
    private StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller = injector.getInstance(StatisticConfigurationEntityMarshaller.class);
    private StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
    private StatisticsMarshaller statisticsMarshaller = injector.getInstance(StatisticsMarshaller.class);

    private String tagEnv = "environment";
    private String valEnvOps = "OPS";
    private String valEnvDev = "DEV";
    private String tagSystem = "system";
    private String valSystemABC = "SystemABC";
    private String valSystemXYZ = "SystemXYZ";

    @Test
    public void testAllTypesAndIntervals() {

        Map<Tuple2<StatisticType, EventStoreTimeIntervalEnum>, String> statNameMap = new HashMap<>();

        //build a map of all the stat names and add them as StatConfig entities
        Arrays.stream(EventStoreTimeIntervalEnum.values()).forEach(interval -> {

            //make sure the purge retention periods are very large so no stats get purged
            stroomPropertyService.setProperty(
                    HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX +
                            interval.name().toLowerCase(), 10_000);

            Arrays.stream(StatisticType.values()).forEach(statisticType -> {
                String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString() + "-" + statisticType + "-" + interval;
                statNameMap.put(new Tuple2(statisticType, interval), statNameStr);
                StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                        statNameStr,
                        statisticType,
                        interval.columnInterval(),
                        StatisticRollUpType.ALL)
                        .addFields(tagEnv, tagSystem)
                        .build();

                StatisticConfigurationEntityHelper.addStatConfig(
                        sessionFactory,
                        statisticConfigurationEntityMarshaller,
                        statisticConfigurationEntity);
            });
        });

        //start at the beginning of today
        ZonedDateTime startTime = ZonedDateTime.now().truncatedTo(ChronoUnit.DAYS);
        LOGGER.info("Start time is {}", startTime);
        int statsInBatch = 10;
        int timeDeltaMs = 500;
        long maxIterations = 10;
        final AtomicLong counter = new AtomicLong(0);
        List<Tuple2<String, String>> valuePairs = new ArrayList<>();
        valuePairs.add(new Tuple2<>(valEnvOps, valSystemABC));
        valuePairs.add(new Tuple2<>(valEnvOps, valSystemXYZ));
        valuePairs.add(new Tuple2<>(valEnvDev, valSystemABC));
        valuePairs.add(new Tuple2<>(valEnvDev, valSystemXYZ));

        List<Statistics.Statistic> statList = new ArrayList<>();

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

        String topicPrefix = stroomPropertyService.getPropertyOrThrow(KafkaStreamService.PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX);

        statNameMap.entrySet().forEach(entry -> {
            String statName = entry.getValue();
            StatisticType statisticType = entry.getKey()._1();
            EventStoreTimeIntervalEnum interval = entry.getKey()._2();

            LOGGER.info("Processing {} - {}", statisticType, interval);

            String topic = TopicNameFactory.getStatisticTypedName(topicPrefix, statisticType);

            ZonedDateTime time = startTime.plus(timeDeltaMs * counter.get(), ChronoUnit.MILLIS);

            Tuple2<String, String> valuePair = valuePairs.get((int) (counter.get() % valuePairs.size()));

            TagType tagTypeEnv = StatisticsHelper.buildTagType(tagEnv, valuePair._1());
            TagType tagTypeSystem = StatisticsHelper.buildTagType(tagSystem, valuePair._2());
            Statistics.Statistic statistic;
            if (statisticType.equals(StatisticType.COUNT)) {
                statistic = StatisticsHelper.buildCountStatistic(statName, time, 10L, tagTypeEnv, tagTypeSystem);
            } else {
                statistic = StatisticsHelper.buildValueStatistic(statName, time, 0.5, tagTypeEnv, tagTypeSystem);
            }

            statList.add(statistic);

            while (counter.get() <= maxIterations) {
                if (counter.get() != 0 && counter.get() % statsInBatch == 0) {
                    Statistics statistics = StatisticsHelper.buildStatistics(statList.toArray(new Statistics.Statistic[statList.size()]));
                    sendStatistics(kafkaProducer, topic, statistics);
                }
                counter.incrementAndGet();
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();

        ThreadUtil.sleep(30_000);

    }


    private void sendStatistics(KafkaProducer<String, String> kafkaProducer, String topic, Statistics statistics) {

        ProducerRecord<String, String> producerRecord = buildProducerRecord(topic, statistics);

        LOGGER.trace(() -> String.format("Sending %s stat events to topic $s", statistics.getStatistic().size(), topic));
        kafkaProducer.send(producerRecord);
    }

    private ProducerRecord<String, String> buildProducerRecord(String topic, Statistics statistics) {
        String statName = statistics.getStatistic().get(0).getName();
        return new ProducerRecord<>(topic, statName, statisticsMarshaller.marshallXml(statistics));
    }

}
