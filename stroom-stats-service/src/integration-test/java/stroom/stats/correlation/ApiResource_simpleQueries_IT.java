package stroom.stats.correlation;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.FieldBuilder;
import stroom.query.api.v2.FlatResult;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.query.api.v2.TableSettings;
import stroom.query.api.v2.TableSettingsBuilder;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.schema.ObjectFactory;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.QueryApiHelper;
import stroom.stats.test.StatisticsHelper;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ApiResource_simpleQueries_IT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiResource_simpleQueries_IT.class);

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
//        Response yesterdayResponse = req().body(() -> searchRequestForYesterday).getStats();
//        SearchResponse yesterdaySearchResponse = yesterdayResponse.readEntity(SearchResponse.class);
        SearchResponse yesterdaySearchResponse = performSearch(searchRequestForYesterday, 3, 60_000);

        dumpRowData(QueryApiHelper.getFlatResult(yesterdaySearchResponse).get(), 50);

        // When 2 - send the query for today
//        Response todayResponse = req().body(() -> searchRequestForToday).getStats();
//        SearchResponse todaySearchResponse = todayResponse.readEntity(SearchResponse.class);
        SearchResponse todaySearchResponse = performSearch(searchRequestForToday, 2, 60_000);

        dumpRowData(QueryApiHelper.getFlatResult(todaySearchResponse).get(), 50);

        // Then 1 - basic checks
        assertThat(QueryApiHelper.getRowCount(yesterdaySearchResponse)).isEqualTo(3);
        assertThat(QueryApiHelper.getRowCount(todaySearchResponse)).isEqualTo(2);

        // Then 2 - correlations
        FlatResult yesterday = ((FlatResult) yesterdaySearchResponse.getResults().get(0));
        FlatResult today = ((FlatResult) todaySearchResponse.getResults().get(0));
        FlatResult yesterdayAndNotToday = new FlatResultCorrelator()
                .addSet("A", yesterday)
                .addSet("B", today)
                .complement("B");

        dumpRowData(yesterdayAndNotToday, 50);

        assertThat(yesterdayAndNotToday.getValues()).hasSize(1);
        assertThat(QueryApiHelper.getStringFieldValues(yesterdayAndNotToday, "user")).contains("user3");
        assertThat(QueryApiHelper.getStringFieldValues(yesterdayAndNotToday, "door")).contains("door1");
    }

    private SearchResponse performSearch(final SearchRequest searchRequest, final int expectedRows, final int timeoutMs) {

        Instant timeoutTime = Instant.now().plusMillis(timeoutMs);
        SearchResponse searchResponse = null;
        long rows = 0;

        do {
            sleep(1_000);
            Response response = req().body(() -> searchRequest).getStats();
            searchResponse = response.readEntity(SearchResponse.class);
            //assume one Result object in the response
            FlatResult flatResult = null;
            if (searchResponse != null && searchResponse.getResults() != null && searchResponse.getResults().size() > 0) {
                rows = ((FlatResult) searchResponse.getResults().get(0)).getSize();
            }
            LOGGER.info("Result count {}, time before timeout {}", rows, Duration.between(Instant.now(), timeoutTime));
        } while ((searchResponse == null || rows < expectedRows) &&
                Instant.now().isBefore(timeoutTime));

        return searchResponse;
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted");
        }
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
                    new FieldBuilder().name(USER_TAG).expression("${" + USER_TAG + "}").build(),
                    new FieldBuilder().name(DOOR_TAG).expression("${" + DOOR_TAG + "}").build()))
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

    private void dumpRowData(final FlatResult flatResult,
                             @Nullable Integer maxRows) {

        Map<String, Class<?>> typeMap = ImmutableMap.of(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Instant.class);

        String tableStr = QueryApiHelper.convertToFixedWidth(flatResult, typeMap, maxRows)
                .stream()
                .collect(Collectors.joining("\n"));

        LOGGER.info("Dumping row data:\n" + tableStr);
    }

}
