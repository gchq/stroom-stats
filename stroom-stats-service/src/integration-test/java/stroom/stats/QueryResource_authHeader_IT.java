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

import org.junit.Before;
import org.junit.Test;
import stroom.stats.api.model.Field;
import stroom.stats.api.model.Filter;
import stroom.stats.api.model.Query;
import stroom.stats.api.model.QueryKey;
import stroom.stats.api.model.ResultRequest;
import stroom.stats.api.model.SearchResponse;
import stroom.stats.api.model.Sort;
import stroom.stats.api.model.TableSettings;
import stroom.stats.api.model.TimeZone;
import stroom.stats.api.QueryApi;
import stroom.stats.api.model.DateTimeFormat;
import stroom.stats.api.model.DocRef;
import stroom.stats.api.model.ExpressionOperator;
import stroom.stats.api.model.ExpressionTerm;
import stroom.stats.api.model.Format;
import stroom.stats.api.model.NumberFormat;
import stroom.stats.api.model.SearchRequest;
import stroom.stats.configuration.StatisticConfiguration;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class QueryResource_authHeader_IT extends AbstractAppIT {

    private String idToken = "eyJhbGciOiJSUzI1NiJ9.eyJleHAiOjE1NDQ4NjMwNjIsInN1YiI6InN0YXRzU2VydmljZVVzZXIiLCJpc3MiOiJzdHJvb20ifQ.7AFPkNgM1UBL_Pj-K5kSNPClgDJ6wZ22WWukjWGw_myZWuMZPGd-kqYxUuQzqqmTA918wylFSx5xBPWA1oCbx0aEPGEOdMnUViykq5XaGEHwPGT9Tf9JI0h8z6-TfOt2VJ2CFsSmRSpfe1CYOywZwziqBwvf5m0rWhfb0wm1abcBnjmX_EfxZiV3McmY0MEzSN6AkYGyWr4ggja06onKEObkoZ9f5pRt7tkTsBpCvaolavfu3IF5FXP9GRifOcxdQXFOgRCDe4JkG6ZAeKbTT6aJJCU798F9jL2ozIw-tQTrA5KjkwIpefaA6CoA_mZd-gIa-ZOLVGzRaIMBo-dl-g";

    /**
     * This test depends on SetupSampleData being run - the DocRef with the uuid needs to exist.
     */
    @Test
    public void testPostQueryData_validCredentials() throws UnsupportedEncodingException, ApiException {
        QueryApi queryApi = new QueryApi(new ApiClient()
                .setBasePath("http://localhost:8086")
                .addDefaultHeader("Authorization", "Bearer " + idToken));

        try {
            SearchRequest searchRequest = getSearchRequest();
            ApiResponse<SearchResponse> response = queryApi.searchWithHttpInfo(getSearchRequest());
            assertThat(response.getStatusCode()).isEqualTo(200);
        } catch (ApiException e){
            String thing = "sdfsd";
            fail();
        }
    }

    @Test
    public void postQueryData_missingCredentials() throws ApiException {
        QueryApi queryApi = new QueryApi(new ApiClient()
                .setBasePath("http://localhost:8086"));

        SearchRequest searchRequest = getSearchRequest();
        ApiResponse<SearchResponse> response = queryApi.searchWithHttpInfo(getSearchRequest());
        assertThat(response.getStatusCode()).isEqualTo(401);
    }

    @Test
    public void postQueryData_invalidCredentials() throws UnsupportedEncodingException, ApiException {
        QueryApi queryApi = new QueryApi(new ApiClient()
                .setBasePath("http://localhost:8086")
                .addDefaultHeader("Authorization", "Bearer " + "GARBAGE"));

        SearchRequest searchRequest = getSearchRequest();
        ApiResponse<SearchResponse> response = queryApi.searchWithHttpInfo(getSearchRequest());
        assertThat(response.getStatusCode()).isEqualTo(401);
    }

    private static SearchRequest getSearchRequest() {
        DocRef docRef = new DocRef()
                .type("docRefType")
                .uuid("e40d59ac-e785-11e6-a678-0242ac120005")
                .name("docRefName");

        ExpressionOperator expressionOperator = new ExpressionOperator()
                .enabled(true)
                .op(ExpressionOperator.OpEnum.AND)
                .children(Arrays.asList(
                    new ExpressionTerm()
                    .field("field1")
                    .condition(ExpressionTerm.ConditionEnum.EQUALS)
                    .value("value1"),
                    new ExpressionTerm()
                            .field("field2")
                            .condition(ExpressionTerm.ConditionEnum.BETWEEN)
                            .value("value2"),
                    new ExpressionTerm()
                            .field(StatisticConfiguration.FIELD_NAME_DATE_TIME)
                            .condition(ExpressionTerm.ConditionEnum.BETWEEN)
                            .value("2017-01-01T00:00:00.000Z,2017-01-31T00:00:00.000Z")
                ));

        Format format = new Format()
                .type(Format.TypeEnum.DATE_TIME)
                .numberFormat(new NumberFormat().decimalPlaces(1).useSeparator(false))
                .dateTimeFormat(new DateTimeFormat()
                        .pattern("yyyy-MM-dd'T'HH:mm:ss")
                        .timeZone(new TimeZone().offsetHours(0).offsetMinutes(0)));

        TableSettings tableSettings = new TableSettings()
                .queryId("someQueryId")
                .fields(Arrays.asList(
                        new Field()
                            .name("name1")
                            .expression("expression1")
                            .sort(new Sort().order(1).direction(Sort.DirectionEnum.ASCENDING))
                            .filter(new Filter().includes("include1").excludes("exclude1"))
                            .format(format)
                            .group(1),
                        new Field()
                                .name("name2")
                                .expression("expression2")
                                .sort(new Sort().order(1).direction(Sort.DirectionEnum.DESCENDING))
                                .filter(new Filter().includes("include2").excludes("exclude2"))
                                .format(format)
                                .group(2)))
                .extractValues(false)
                .extractionPipeline(new DocRef().type("docRefType2").uuid("docRefUuid2").name("docRefName2"))
                .maxResults(Arrays.asList(1, 2))
                .showDetail(false);

        ResultRequest resultRequest = new ResultRequest().componentId("componentId").mappings(Arrays.asList(tableSettings));
        Query query = new Query().dataSource(docRef).expression(expressionOperator);

        SearchRequest searchRequest = new SearchRequest()
            .key(new QueryKey().uuid("queryKeyUuid"))
            .query(query)
            .resultRequests(Arrays.asList(resultRequest))
            .dateTimeLocale("en-gb")
            .incremental(false);
        return searchRequest;
    }
}
