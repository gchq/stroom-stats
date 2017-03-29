package stroom.stats.testdata;

import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.schema.ObjectFactory;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticsHelper;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

public class TestData {
    private static final String USER_TAG = "user";
    private static final String DOOR_TAG = "door";

    public static DummyStat usersEnteringTheBuilding(ZonedDateTime dateTime) {
        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.DAY;
        String statName = "UsersEnteringTheBuilding-" + dateTime.toString() + "-" + statisticType + "-" + interval;

        StatisticConfigurationEntity statisticConfigurationEntity =
                createDummyStatisticConfiguration(statName, StatisticType.COUNT, EventStoreTimeIntervalEnum.DAY, USER_TAG, DOOR_TAG);

        Statistics statistics = new ObjectFactory().createStatistics();
        statistics.getStatistic().addAll(buildStatsForYesterday(statName, dateTime));
        statistics.getStatistic().addAll(buildStatsForToday(statName, dateTime));

        return new DummyStat()
                .statistics(statistics)
                .statisticConfigurationEntity(statisticConfigurationEntity)
                .tags(USER_TAG, DOOR_TAG)
                .time(dateTime);
    }

    private static List<Statistics.Statistic> buildStatsForYesterday(String statName, ZonedDateTime now) {
        return Arrays.asList(
                StatisticsHelper.buildCountStatistic(
                        statName, now.minusDays(1), 1,
                        StatisticsHelper.buildTagType(USER_TAG, "user1"),
                        StatisticsHelper.buildTagType(DOOR_TAG, "door1")),

                StatisticsHelper.buildCountStatistic(
                        statName, now.minusDays(1), 1,
                        StatisticsHelper.buildTagType(USER_TAG, "user2"),
                        StatisticsHelper.buildTagType(DOOR_TAG, "door1")),


                StatisticsHelper.buildCountStatistic(
                        statName, now.minusDays(1), 1,
                        StatisticsHelper.buildTagType(USER_TAG, "user3"),
                        StatisticsHelper.buildTagType(DOOR_TAG, "door1"))
        );
    }

    private static List<Statistics.Statistic> buildStatsForToday(String statName, ZonedDateTime now) {
        return Arrays.asList(
                StatisticsHelper.buildCountStatistic(
                        statName, now, 1,
                        StatisticsHelper.buildTagType(USER_TAG, "user1"),
                        StatisticsHelper.buildTagType(DOOR_TAG, "door1")),

                StatisticsHelper.buildCountStatistic(
                        statName, now, 1,
                        StatisticsHelper.buildTagType(USER_TAG, "user2"),
                        StatisticsHelper.buildTagType(DOOR_TAG, "door1"))
        );
    }


    private static StatisticConfigurationEntity createDummyStatisticConfiguration(
            String statName, StatisticType statisticType, EventStoreTimeIntervalEnum interval, String... fields) {
        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                statName,
                statisticType,
                interval.columnInterval(),
                StatisticRollUpType.ALL)
                .addFields(fields)
                .build();

        return statisticConfigurationEntity;
    }
}
