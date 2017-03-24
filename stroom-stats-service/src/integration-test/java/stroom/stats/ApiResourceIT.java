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
import stroom.query.api.Sort;
import stroom.query.api.TableSettings;
import stroom.query.api.TimeZone;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.schema.Statistics;

import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static stroom.query.api.ExpressionTerm.Condition;
import static stroom.stats.HttpAsserts.assertAccepted;
import static stroom.stats.HttpAsserts.assertUnauthorized;

public class ApiResourceIT extends AbstractAppIT {

    @Test
    public void testPostEmptyStatistics() throws UnsupportedEncodingException {
        Response response = postXml(Statistics::new, STATISTICS_URL, AuthorizationHelper::getHeaderWithValidCredentials);
        assertAccepted(response);
    }

    @Test
    public void postEmptyStatistics_missingCredentials() {
        Response response = postXml(Statistics::new, STATISTICS_URL, AuthorizationHelper::getHeaderWithInvalidCredentials);
        assertUnauthorized(response);
    }

    @Test
    public void postEmptyStatistics_invalidCredentials() throws UnsupportedEncodingException {
        Response response = postXml(ApiResourceIT::getSearchRequest, STATISTICS_URL, AuthorizationHelper::getHeaderWithInvalidCredentials);
        assertUnauthorized(response);
    }

    /**
     * This test depends on SetupSampleData being run - the DocRef with the uuid needs to exist.
     */
    @Test
    public void testPostQueryData() throws UnsupportedEncodingException {
        Response response = postJson(ApiResourceIT::getSearchRequest, QUERY_URL, AuthorizationHelper::getHeaderWithValidCredentials);
        assertAccepted(response);
    }

    @Test
    public void postQueryData_missingCredentials(){
        Response response = postJson(ApiResourceIT::getSearchRequest, QUERY_URL, () -> "");
        assertUnauthorized(response);
    }

    @Test
    public void postQueryData_invalidCredentials() throws UnsupportedEncodingException {
        Response response = postJson(ApiResourceIT::getSearchRequest, QUERY_URL, AuthorizationHelper::getHeaderWithInvalidCredentials);
        assertUnauthorized(response);
    }

    private static SearchRequest getSearchRequest() {
        DocRef docRef = new DocRef("docRefType", "e40d59ac-e785-11e6-a678-0242ac120005", "docRefName");

        ExpressionOperator expressionOperator = new ExpressionOperator(
                true,
                ExpressionOperator.Op.AND,
                new ExpressionTerm("field1", Condition.EQUALS, "value1"),
                new ExpressionTerm("field2", Condition.BETWEEN, "value2"),
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, "2017-01-01T00:00:00.000Z,2017-01-31T00:00:00.000Z")
        );

        Format format = new Format(
                Format.Type.DATE_TIME,
                new NumberFormat(1, false),
                new DateTimeFormat("yyyy-MM-dd'T'HH:mm:ss", TimeZone.fromOffset(0, 0)));

        TableSettings tableSettings = new TableSettings(
                "someQueryId",
                Arrays.asList(
                        new Field(
                                "name1",
                                "expression1",
                                new Sort(1, Sort.SortDirection.ASCENDING),
                                new Filter("include1", "exclude1"),
                                format,
                                1),
                        new Field(
                                "name2",
                                "expression2",
                                new Sort(2, Sort.SortDirection.DESCENDING),
                                new Filter("include2", "exclude2"),
                                format,
                                2)),
                false,
                new DocRef("docRefType2", "docRefUuid2", "docRefName2"),
                Arrays.asList(1, 2),
                false
        );

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
