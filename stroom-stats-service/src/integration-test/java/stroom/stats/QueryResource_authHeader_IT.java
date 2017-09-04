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

import org.junit.Ignore;
import org.junit.Test;
import stroom.query.api.v2.DateTimeFormat;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.Filter;
import stroom.query.api.v2.Format;
import stroom.query.api.v2.NumberFormat;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.Sort;
import stroom.query.api.v2.TableSettings;
import stroom.query.api.v2.TimeZone;
import stroom.stats.configuration.StatisticConfiguration;

import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static stroom.query.api.v2.ExpressionTerm.Condition;
import static stroom.stats.HttpAsserts.assertAccepted;
import static stroom.stats.HttpAsserts.assertUnauthorized;

public class QueryResource_authHeader_IT extends AbstractAppIT {

//    @Test
//    public void postEmptyStatistics_validCredentials() throws UnsupportedEncodingException {
//        Response response = req().useXml().body(Statistics::new).postStats();
//        assertAccepted(response);
//    }
//
//    @Test
//    public void postEmptyStatistics_missingCredentials() {
//        Response response = req().useXml().body(Statistics::new).authHeader(AuthHeader.MISSING).postStats();
//        assertUnauthorized(response);
//    }
//
//    @Test
//    public void postEmptyStatistics_invalidCredentials() throws UnsupportedEncodingException {
//        Response response = req().useXml().body(Statistics::new).authHeader(AuthHeader.INVALID).postStats();
//        assertUnauthorized(response);
//    }

    /**
     * This test depends on SetupSampleData being run - the DocRef with the uuid needs to exist.
     */
    @Test
    public void testPostQueryData_validCredentials() throws UnsupportedEncodingException {
        Response response = req().body(QueryResource_authHeader_IT::getSearchRequest).getStats();
        assertAccepted(response);
    }

    @Test
    public void postQueryData_missingCredentials(){
        Response response = req().body(QueryResource_authHeader_IT::getSearchRequest).authHeader(AuthHeader.MISSING).getStats();
        assertUnauthorized(response);
    }

    @Test
    public void postQueryData_invalidCredentials() throws UnsupportedEncodingException {
        Response response = req().body(QueryResource_authHeader_IT::getSearchRequest).authHeader(AuthHeader.INVALID).getStats();
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
