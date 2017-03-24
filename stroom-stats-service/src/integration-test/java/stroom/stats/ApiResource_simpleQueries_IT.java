package stroom.stats;

import org.junit.Test;
import stroom.query.api.DateTimeFormat;
import stroom.query.api.DocRef;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.Field;
import stroom.query.api.Filter;
import stroom.query.api.Format;
import stroom.query.api.NumberFormat;
import stroom.query.api.Query;
import stroom.query.api.QueryKey;
import stroom.query.api.ResultRequest;
import stroom.query.api.SearchRequest;
import stroom.query.api.SearchResponse;
import stroom.query.api.Sort;
import stroom.query.api.TableSettings;
import stroom.query.api.TimeZone;
import stroom.stats.configuration.StatisticConfiguration;

import javax.ws.rs.core.Response;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class ApiResource_simpleQueries_IT extends AbstractAppIT {

    @Test
    public void test(){
        Response response = req().body(ApiResource_simpleQueries_IT::getSearchRequest).getStats();
        HttpAsserts.assertAccepted(response);
        SearchResponse searchResponse = response.readEntity(SearchResponse.class);
        assertThat(searchResponse).isNotNull();
    }

    private static SearchRequest getSearchRequest() {
        // TODO: it makes no sense that this is a DocRef. API users won't care that it's a stroom entity thing. It should be StatisticConfigurationId or something like that.
        DocRef docRef = new DocRef("docRefType", "623111ae-a9cf-42a4-9075-a1c036d52c6c", "623111ae-a9cf-42a4-9075-a1c036d52c6c");

        ExpressionOperator expressionOperator = new ExpressionOperator(
                true,
                ExpressionOperator.Op.AND,
                new ExpressionTerm("environment", ExpressionTerm.Condition.EQUALS, "OPS"),
                new ExpressionTerm("system", ExpressionTerm.Condition.EQUALS, "SystemABC"),
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, ExpressionTerm.Condition.BETWEEN, "2017-01-01T00:00:00.000Z,2017-05-31T00:00:00.000Z")
        );

        //TODO: why do I need to pass a format?
        Format format = new Format(
                Format.Type.DATE_TIME,
                new NumberFormat(1, false),
                new DateTimeFormat("yyyy-MM-dd'T'HH:mm:ss", TimeZone.fromOffset(0, 0)));

        // TODO Why do I need to pass a table setting? Can't it infer the output or something?
        TableSettings tableSettings = new TableSettings(
                "someQueryId",
                Arrays.asList(),
//                        new Field(
//                                "name1",
//                                "expression1",
//                                new Sort(1, Sort.SortDirection.ASCENDING),
//                                new Filter("include1", "exclude1"),
//                                format,
//                                1),
//                        new Field(
//                                "name2",
//                                "expression2",
//                                new Sort(2, Sort.SortDirection.DESCENDING),
//                                new Filter("include2", "exclude2"),
//                                format,
//                                2)),
                false,
                new DocRef("docRefType2", "docRefUuid2", "docRefName2"),
                Arrays.asList(1, 2),
                false
        );

        // TODO what's a result request and why do I care?
        ResultRequest resultRequest = new ResultRequest("componentId", tableSettings);

        Query query = new Query(docRef, expressionOperator);

        SearchRequest searchRequest = new SearchRequest(
                new QueryKey("queryKeyUuid"),
                query,
                Arrays.asList(resultRequest),
                "en-gb",
                false);

        return searchRequest;
    }
}
