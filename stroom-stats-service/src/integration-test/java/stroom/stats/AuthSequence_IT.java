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
import org.hibernate.SessionFactory;
import org.junit.Test;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.FieldBuilder;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.QueryKey;
import stroom.query.api.v2.ResultRequest;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.TableSettings;
import stroom.query.api.v2.TableSettingsBuilder;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StroomStatsStoreEntityHelper;

import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static stroom.stats.HttpAsserts.assertOk;

public class AuthSequence_IT extends AbstractAppIT {

    private Injector injector = getApp().getInjector();

    private final static String TAG_ENV = "environment";
    private final static String TAG_SYSTEM = "system";
    private static final String USER_TAG = "user";
    private static final String DOOR_TAG = "door";

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

        String securityToken = AuthorizationHelper.login();

        Response response = req()
                .body(() -> getUsersDoorsRequest(statisticConfigurationUuid, getDateRangeFor(ZonedDateTime.now())))
                .jwtToken(securityToken)
                .getStats();
        assertOk(response);
    }

    private static List<StatisticConfiguration> createDummyStatisticConfigurations(){
        List<StatisticConfiguration> stats = new ArrayList<>();
        stats.add(StroomStatsStoreEntityHelper.createDummyStatisticConfiguration(
                "AuthSequence_IT-", StatisticType.COUNT, EventStoreTimeIntervalEnum.SECOND, TAG_ENV, TAG_SYSTEM));
        stats.add(StroomStatsStoreEntityHelper.createDummyStatisticConfiguration(
                "AuthSequence_IT-", StatisticType.VALUE, EventStoreTimeIntervalEnum.SECOND, TAG_ENV, TAG_SYSTEM));
        return stats;
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

    private static String getDateRangeFor(ZonedDateTime dateTime){
        String day = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String range= String.format("%sT00:00:00.000Z,%sT23:59:59.000Z", day, day);
        return range;
    }
}
