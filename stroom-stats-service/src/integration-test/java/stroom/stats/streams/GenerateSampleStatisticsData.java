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

import javaslang.Tuple2;
import javaslang.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticsHelper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class GenerateSampleStatisticsData {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateSampleStatisticsData.class);

    public static final String TAG_USER = "user";
    public static final String TAG_COLOUR = "colour";
    public static final String TAG_STATE = "state";
    public static final String[] TAGS = {TAG_USER, TAG_COLOUR, TAG_STATE};

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";

    public static final int EVENT_TIME_DELTA_MS = 5_000;

    private static final String COLOUR_RED = "Red";
    private static final String COLOUR_GREEN = "Green";
    private static final String COLOUR_BLUE = "Blue";

    public static final List<String> COLOURS = Arrays.asList(COLOUR_RED, COLOUR_GREEN, COLOUR_BLUE);

    public static final List<String> STATES = Arrays.asList("IN", "OUT");

    public static final String[] USERS = new String[]{USER1, USER2};

    public static final long COUNT_STAT_VALUE = 1L;

    public static final Map<String, Double> VALUE_STAT_VALUE_MAP = new HashMap<>();

    static {
        //values such that all three add up to a nice round number
        VALUE_STAT_VALUE_MAP.put(COLOUR_RED, 10.1);
        VALUE_STAT_VALUE_MAP.put(COLOUR_GREEN, 20.2);
        VALUE_STAT_VALUE_MAP.put(COLOUR_BLUE, 69.7);
    }


    private static ZonedDateTime getStartTime() {
        return ZonedDateTime.ofInstant(Instant.now().truncatedTo(ChronoUnit.DAYS), ZoneOffset.UTC);
    }

    public static Tuple2<StatisticConfigurationEntity, List<Statistics>> generateData(
            String statName,
            StatisticType statisticType,
            EventStoreTimeIntervalEnum smallestInterval,
            StatisticRollUpType statisticRollUpType,
            int batchSize,
            int iterationCount) {

        //build the stat config for the stats we are about to generate
        StatisticConfigurationEntity statisticConfiguration = new StatisticConfigurationEntityBuilder(
                statName,
                statisticType,
                smallestInterval.columnInterval(),
                statisticRollUpType)
                .addFields(TAGS)
                .build();

        final ZonedDateTime eventTime = getStartTime();
        //generate the stat events
        List<Statistics.Statistic> statisticList = buildEvents(statName, eventTime, statisticType, iterationCount);

        //randomise the stats
        Collections.shuffle(statisticList, new Random());

        //group the stats in to batches so we can wrap each batch into a Statistics object
        AtomicInteger counter = new AtomicInteger(0);
        Map<Integer, List<Statistics.Statistic>> batches = statisticList.parallelStream()
                .map(statistic -> new Tuple2<>(counter.getAndIncrement() % batchSize, statistic))
                .collect(Collectors.groupingBy(Tuple2::_1, Collectors.mapping(Tuple2::_2, Collectors.toList())));

        List<Statistics> batchList = batches.values().parallelStream()
                .map(StatisticsHelper::buildStatistics)
                .collect(Collectors.toList());

        return new Tuple2<>(statisticConfiguration, batchList);
    }


    private static List<Statistics.Statistic> buildEvents(final String statName,
                                                          final ZonedDateTime initialEventTime,
                                                          final StatisticType statisticType,
                                                          final int iterationCount) {
        ZonedDateTime eventTime = initialEventTime;

        List<Statistics.Statistic> statisticList = new ArrayList<>(iterationCount);

        List<Tuple4<ZonedDateTime, String, String, String>> perms = new ArrayList<>();

        for (int i = 0; i < iterationCount; i++) {
            for (final String user : USERS) {
                for (final String colour : COLOURS) {
                    for (final String state : STATES) {

                        Statistics.Statistic statistic;
                        if (statisticType.equals(StatisticType.COUNT)) {
                            statistic = StatisticsHelper.buildCountStatistic(
                                    statName,
                                    eventTime,
                                    COUNT_STAT_VALUE,
                                    StatisticsHelper.buildTagType(TAG_USER, user),
                                    StatisticsHelper.buildTagType(TAG_COLOUR, colour),
                                    StatisticsHelper.buildTagType(TAG_STATE, state));

                        } else if (statisticType.equals(StatisticType.VALUE)) {
                            //give each different colour a different value
                            double val = VALUE_STAT_VALUE_MAP.get(colour);

                            statistic = StatisticsHelper.buildValueStatistic(
                                    statName,
                                    eventTime,
                                    val,
                                    StatisticsHelper.buildTagType(TAG_USER, user),
                                    StatisticsHelper.buildTagType(TAG_COLOUR, colour),
                                    StatisticsHelper.buildTagType(TAG_STATE, state));
                        } else {
                            throw new RuntimeException(String.format("Unexpected type %s", statisticType));
                        }

                        statisticList.add(statistic);

                        perms.add(new Tuple4<>(eventTime, user, colour, state));
                    }
                }
            }
            eventTime = eventTime.plus(EVENT_TIME_DELTA_MS, ChronoUnit.MILLIS);
        }

        long distinctCount = perms.stream()
                .distinct()
                .count();

        LOGGER.info("Returning {} statistic events, ({} distinct)", statisticList.size(), distinctCount);
        return statisticList;
    }
}
