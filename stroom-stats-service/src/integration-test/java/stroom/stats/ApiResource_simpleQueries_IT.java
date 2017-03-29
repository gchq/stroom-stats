package stroom.stats;

import com.google.inject.Injector;
import javaslang.control.Try;
import org.hibernate.SessionFactory;
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
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.streams.KafkaStreamService;
import stroom.stats.streams.TopicNameFactory;
import stroom.stats.test.StatisticConfigurationEntityHelper;
import stroom.stats.testdata.DummyStat;
import stroom.stats.testdata.KafkaHelper;
import stroom.stats.testdata.TestData;
import stroom.stats.xml.StatisticsMarshaller;
import stroom.util.thread.ThreadUtil;

import javax.ws.rs.core.Response;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ApiResource_simpleQueries_IT extends AbstractAppIT {

    private Injector injector = getApp().getInjector();

    @Test
    public void test1(){
        // Given 1 - setup data
        ZonedDateTime now = ZonedDateTime.now();
        DummyStat usersEnteringTheBuildingData = setupStatisticConfigurations(now, injector);
        sendStatistics(usersEnteringTheBuildingData, injector);

        // Given 2 - get queries together
        String uuid = usersEnteringTheBuildingData.statisticConfigurationEntity().getUuid();
        SearchRequest searchRequestForYesterday = getUsersDoorsRequest(uuid, getDateRangeFor(now.minusDays(1)));
        SearchRequest searchRequestForToday = getUsersDoorsRequest(uuid, getDateRangeFor(now));

        // When
        Response yesterdayResponse = req().body(() -> searchRequestForYesterday).getStats();
        SearchResponse yesterdaySearchResponse = yesterdayResponse.readEntity(SearchResponse.class);

        Response todayResponse = req().body(() -> searchRequestForToday).getStats();
        SearchResponse todaySearchResponse = todayResponse.readEntity(SearchResponse.class);

        // Then
        assertThat(((TableResult)yesterdaySearchResponse.getResults().get(0)).getTotalResults()).isEqualTo(3);
        assertThat(((TableResult)todaySearchResponse.getResults().get(0)).getTotalResults()).isEqualTo(2);

        List<Row> yesterday = ((TableResult) yesterdaySearchResponse.getResults().get(0)).getRows();
        List<Row> today = ((TableResult) todaySearchResponse.getResults().get(0)).getRows();

        // TODO assert on the correlation
    }

    private static String getDateRangeFor(ZonedDateTime dateTime){
        String day = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String range= String.format("%sT00:00:00.000Z,%sT23:59:59.000Z", day, day);
        return range;
//        return "2017-01-01T00:00:00.000Z,2017-12-30T00:00:00.000Z";
    }


    private static DummyStat setupStatisticConfigurations(ZonedDateTime dateTime, Injector injector){
        SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
        StatisticConfigurationEntityMarshaller statisticConfigurationMarshaller = injector.getInstance(StatisticConfigurationEntityMarshaller.class);
        DummyStat usersEnteringTheBuildingData = TestData.usersEnteringTheBuilding(dateTime);
        Try<StatisticConfigurationEntity>  statisticConfigurationEntity = StatisticConfigurationEntityHelper.addStatConfig(
                sessionFactory, statisticConfigurationMarshaller, usersEnteringTheBuildingData.statisticConfigurationEntity());
        usersEnteringTheBuildingData.statisticConfigurationEntity(statisticConfigurationEntity.get());
        return usersEnteringTheBuildingData;
    }

    private static void sendStatistics(DummyStat usersEnteringTheBuildingData, Injector injector){
        StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
        StatisticsMarshaller statisticsMarshaller = injector.getInstance(StatisticsMarshaller.class);
        String topicPrefix = stroomPropertyService.getPropertyOrThrow(KafkaStreamService.PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX);
        String topic = TopicNameFactory.getStatisticTypedName(topicPrefix, usersEnteringTheBuildingData.statisticConfigurationEntity().getStatisticType());
        KafkaHelper.sendDummyStatistics(
                KafkaHelper.buildKafkaProducer(stroomPropertyService),
                topic,
                Arrays.asList(usersEnteringTheBuildingData.statistics()),
                statisticsMarshaller);
        // Waiting for a bit so that we know the Statistics have been processed
        ThreadUtil.sleep(60_000);
        //TODO it'd be useful to check HBase and see if the stats have been created
    }

    private static SearchRequest getUsersDoorsRequest(String statisticConfigurationUuid, String timeConstraint) {

        ExpressionOperator expressionOperator = new ExpressionOperator(
                true,
                ExpressionOperator.Op.AND,
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
