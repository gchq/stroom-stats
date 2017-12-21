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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class QueryResource_authHeader_IT extends AbstractAppIT {

    private String idToken = "eyJhbGciOiJSUzI1NiJ9.eyJleHAiOjE1NDQ4NjMwNjIsInN1YiI6InN0YXRzU2VydmljZVVzZXIiLCJpc3MiOiJzdHJvb20ifQ.7AFPkNgM1UBL_Pj-K5kSNPClgDJ6wZ22WWukjWGw_myZWuMZPGd-kqYxUuQzqqmTA918wylFSx5xBPWA1oCbx0aEPGEOdMnUViykq5XaGEHwPGT9Tf9JI0h8z6-TfOt2VJ2CFsSmRSpfe1CYOywZwziqBwvf5m0rWhfb0wm1abcBnjmX_EfxZiV3McmY0MEzSN6AkYGyWr4ggja06onKEObkoZ9f5pRt7tkTsBpCvaolavfu3IF5FXP9GRifOcxdQXFOgRCDe4JkG6ZAeKbTT6aJJCU798F9jL2ozIw-tQTrA5KjkwIpefaA6CoA_mZd-gIa-ZOLVGzRaIMBo-dl-g";

    /**
     * This test depends on SetupSampleData being run - the DocRef with the uuid needs to exist.
     */
    @Test
    public void testPostQueryData_validCredentials() throws UnsupportedEncodingException {
        Response response = getClient().target("http://localhost:8086/api/stroom-stats/v2/search")
                .request()
                .header("Authorization", "Bearer " + idToken)
                .post(Entity.json(getSearchRequest()));

        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    public void postQueryData_missingCredentials() {
        Response response = getClient().target("http://localhost:8086/api/stroom-stats/v2/search")
                .request()
                .post(Entity.json(getSearchRequest()));

        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    public void postQueryData_invalidCredentials() throws UnsupportedEncodingException {
        Response response = getClient().target("http://localhost:8086/api/stroom-stats/v2/search")
                .request()
                .header("Authorization", "Bearer " + "GARBAGE")
                .post(Entity.json(getSearchRequest()));

        assertThat(response.getStatus()).isEqualTo(401);
    }

    private static SearchRequest getSearchRequest() {
        DocRef docRef = new DocRef.Builder()
                .type("docRefType")
                .uuid("e40d59ac-e785-11e6-a678-0242ac120005")
                .name("docRefName")
                .build();

        Format format = new Format.Builder()
                .type(Format.Type.DATE_TIME)
                .number(new NumberFormat.Builder().decimalPlaces(1).useSeparator(false).build())
                .dateTime(new DateTimeFormat.Builder()
                        .pattern("yyyy-MM-dd'T'HH:mm:ss")
                        .timeZone(new TimeZone.Builder().offsetHours(0).offsetMinutes(0).build())
                        .build())
                .build();

        ResultRequest resultRequest = new ResultRequest.Builder()
                .componentId("componentId")
                .addMappings(
                    new TableSettings.Builder()
                        .queryId("someQueryId")
                        .addFields(
                            new Field.Builder()
                                .name("name1")
                                .expression("expression1")
                                .sort(new Sort.Builder().order(1).direction(Sort.SortDirection.ASCENDING).build())
                                .filter(new Filter.Builder().includes("include1").excludes("exclude1").build())
                                .format(format)
                                .group(1)
                                .build(),
                            new Field.Builder()
                                .name("name2")
                                .expression("expression2")
                                .sort(new Sort.Builder().order(1).direction(Sort.SortDirection.ASCENDING).build())
                                .filter(new Filter.Builder().includes("include2").excludes("exclude2").build())
                                .format(format)
                                .group(1)
                                .build())
                        .extractValues(false)
                        .extractionPipeline(new DocRef.Builder().type("docRefType2").uuid("docRefUuid2").name("docRefName2").build())
                        .addMaxResults(1, 2)
                        .showDetail(false)
                        .build())
                .build();

        Query query = new Query.Builder()
                .dataSource(docRef)
                .expression(
                        new ExpressionOperator.Builder(ExpressionOperator.Op.AND)
                            .addTerm("fieldX", ExpressionTerm.Condition.EQUALS, "abc")
                            .addOperator(new ExpressionOperator.Builder(ExpressionOperator.Op.OR)
                                    .addTerm("fieldA", ExpressionTerm.Condition.EQUALS, "Fred")
                                    .addTerm("fieldA", ExpressionTerm.Condition.EQUALS, "Fred")
                                    .build())
                            .addTerm("fieldY", ExpressionTerm.Condition.BETWEEN, "10,20")
                        .build())
                .build();

        SearchRequest searchRequest = new SearchRequest.Builder()
            .key(new QueryKey("queryKeyUuid"))
            .query(query)
            .addResultRequests(resultRequest)
            .dateTimeLocale("en-gb")
            .incremental(false)
            .build();
        return searchRequest;
    }
}
