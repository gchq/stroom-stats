package stroom.stats.correlation;

import com.google.inject.Injector;
import org.junit.Test;
import stroom.query.api.DocRef;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.FieldBuilder;
import stroom.query.api.Query;
import stroom.query.api.QueryKey;
import stroom.query.api.ResultRequest;
import stroom.query.api.Row;
import stroom.query.api.SearchRequest;
import stroom.query.api.SearchResponse;
import stroom.query.api.TableResult;
import stroom.query.api.TableSettings;
import stroom.query.api.TableSettingsBuilder;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.schema.ObjectFactory;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticsHelper;

import javax.ws.rs.core.Response;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ApiResource_simpleQueries_IT extends AbstractAppIT {

    private Injector injector = getApp().getInjector();

    private static final String USER_TAG = "user";
    private static final String DOOR_TAG = "door";

    @Test
    public void compare_yesterday_to_today() throws InterruptedException {
        // Given 1 - create a StatisticConfiguration, create and send Statistics
        ZonedDateTime now = ZonedDateTime.now();
        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.DAY;
        String statName = "UsersEnteringTheBuilding-" + now.toString() + "-" + statisticType + "-" + interval;
        String statisticConfigurationUuid = StatisticConfigurationCreator.create(injector, statName, statisticType, interval, USER_TAG, DOOR_TAG);
        StatisticSender.sendStatistics(injector, getStats(statName, now), statisticType);

        // Given 2 - get queries ready
        SearchRequest searchRequestForYesterday = getUsersDoorsRequest(statisticConfigurationUuid, getDateRangeFor(now.minusDays(1)));
        SearchRequest searchRequestForToday = getUsersDoorsRequest(statisticConfigurationUuid, getDateRangeFor(now));

        // When 1 - send the query for yesterday
        Response yesterdayResponse = req().body(() -> searchRequestForYesterday).getStats();
        SearchResponse yesterdaySearchResponse = yesterdayResponse.readEntity(SearchResponse.class);

        // When 2 - send the query for today
        Response todayResponse = req().body(() -> searchRequestForToday).getStats();
        SearchResponse todaySearchResponse = todayResponse.readEntity(SearchResponse.class);

        // Then 1 - basic checks
        assertThat(((TableResult)yesterdaySearchResponse.getResults().get(0)).getTotalResults()).isEqualTo(3);
        assertThat(((TableResult)todaySearchResponse.getResults().get(0)).getTotalResults()).isEqualTo(2);

        // Then 2 - correlations
        List<Row> yesterday = ((TableResult) yesterdaySearchResponse.getResults().get(0)).getRows();
        List<Row> today = ((TableResult) todaySearchResponse.getResults().get(0)).getRows();
        List<Row> yesterdayAndNotToday = new Correlator()
                .addSet("A", new HashSet<>(yesterday))
                .addSet("B", new HashSet<>(today))
                .complement("B");
        assertThat(yesterdayAndNotToday.size()).isEqualTo(1);
        assertThat(yesterdayAndNotToday.get(0).getValues().get(0)).isEqualTo("user3");
        assertThat(yesterdayAndNotToday.get(0).getValues().get(1)).isEqualTo("door1");
    }

    private static String getDateRangeFor(ZonedDateTime dateTime){
        String day = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String range= String.format("%sT00:00:00.000Z,%sT23:59:59.000Z", day, day);
        return range;
    }

    private static Statistics getStats(String statName, ZonedDateTime dateTime){
        Statistics statistics = new ObjectFactory().createStatistics();
        statistics.getStatistic().addAll(buildStatsForYesterday(statName, dateTime));
        statistics.getStatistic().addAll(buildStatsForToday(statName, dateTime));
        return statistics;
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



    private static SearchRequest getUsersDoorsRequest(String statisticConfigurationUuid, String timeConstraint) {

        ExpressionOperator expressionOperator = new ExpressionOperator(
                true,
                ExpressionOperator.Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION,
                        ExpressionTerm.Condition.EQUALS,
                        EventStoreTimeIntervalEnum.DAY.longName()),
                new ExpressionTerm("door", ExpressionTerm.Condition.EQUALS, "door1"),
                new ExpressionTerm(
                        StatisticConfiguration.FIELD_NAME_DATE_TIME,
                        ExpressionTerm.Condition.BETWEEN,
                        timeConstraint)
        );

        TableSettings tableSettings = new TableSettingsBuilder()
                .fields(Arrays.asList(
                    new FieldBuilder().name("User").expression("${user}").build(),
                    new FieldBuilder().name("Door").expression("${door}").build()))
                . build();

        ResultRequest resultRequest = new ResultRequest("mainResult", tableSettings);
        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfigurationUuid, statisticConfigurationUuid),
                expressionOperator);

        SearchRequest searchRequest = new SearchRequest(
                new QueryKey(UUID.randomUUID().toString()),
                query,
                Arrays.asList(resultRequest),
                "en-gb",
                false);

        return searchRequest;
    }

}
