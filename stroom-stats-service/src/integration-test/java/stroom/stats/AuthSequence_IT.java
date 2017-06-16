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

import com.google.inject.Injector;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientResponse;
import org.hibernate.SessionFactory;
import org.junit.Ignore;
import org.junit.Test;
import stroom.query.api.v1.DateTimeFormat;
import stroom.query.api.v1.DocRef;
import stroom.query.api.v1.ExpressionOperator;
import stroom.query.api.v1.ExpressionTerm;
import stroom.query.api.v1.Field;
import stroom.query.api.v1.Filter;
import stroom.query.api.v1.Format;
import stroom.query.api.v1.NumberFormat;
import stroom.query.api.v1.Query;
import stroom.query.api.v1.QueryKey;
import stroom.query.api.v1.ResultRequest;
import stroom.query.api.v1.SearchRequest;
import stroom.query.api.v1.Sort;
import stroom.query.api.v1.TableSettings;
import stroom.query.api.v1.TimeZone;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StroomStatsStoreEntityHelper;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static stroom.query.api.v1.ExpressionTerm.Condition;
import static stroom.stats.HttpAsserts.assertAccepted;


//TODO fix SearchRequest
@Ignore("SearchRequest is invalid")
public class AuthSequence_IT extends AbstractAppIT {

    private Injector injector = getApp().getInjector();

    private final static String TAG_ENV = "environment";
    private final static String TAG_SYSTEM = "system";

    private String statisticConfigurationUuid;

    /**
     * This test depends on SetupSampleData being run - the DocRef with the uuid needs to exist.
     */
    @Test
    public void testPostQueryData_validCredentials() throws UnsupportedEncodingException {

        List<StatisticConfiguration> statisticConfigurations = createDummyStatisticConfigurations();

        List<String> uuids = StroomStatsStoreEntityHelper.persistDummyStatisticConfigurations(
                statisticConfigurations,
                injector.getInstance(SessionFactory.class),
                injector.getInstance(StroomStatsStoreEntityMarshaller.class));
        statisticConfigurationUuid = uuids.get(0);

        String jwtToken = loginToStroomAsAdmin();

        Response response = req()
                .body(this::getSearchRequest)
                .jwtToken(jwtToken)
                .getStats();
        assertAccepted(response);
    }

    private static List<StatisticConfiguration> createDummyStatisticConfigurations(){
        List<StatisticConfiguration> stats = new ArrayList<>();
        stats.add(StroomStatsStoreEntityHelper.createDummyStatisticConfiguration(
                "AuthSequence_IT-", StatisticType.COUNT, EventStoreTimeIntervalEnum.SECOND, TAG_ENV, TAG_SYSTEM));
        stats.add(StroomStatsStoreEntityHelper.createDummyStatisticConfiguration(
                "AuthSequence_IT-", StatisticType.VALUE, EventStoreTimeIntervalEnum.SECOND, TAG_ENV, TAG_SYSTEM));
        return stats;
    }

    private String loginToStroomAsAdmin(){
        Client client = ClientBuilder.newClient(new ClientConfig().register(ClientResponse.class));
        Response response = client
                .target("http://localhost:8080/api/authentication/getToken")
                .request()
                .header("Authorization", AuthorizationHelper.getHeaderWithValidBasicAuthCredentials())
                .get();
        String jwtToken = response.readEntity(String.class);
        return jwtToken;
    }

    private SearchRequest getSearchRequest() {
        DocRef docRef = new DocRef("docRefType", statisticConfigurationUuid, "docRefName");

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
