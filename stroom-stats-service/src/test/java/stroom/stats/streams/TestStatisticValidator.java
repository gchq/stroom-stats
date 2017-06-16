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

package stroom.stats.streams;

import org.junit.Test;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticsHelper;

import javax.xml.datatype.DatatypeConfigurationException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatisticValidator {
    String statName = "MyStat";

    @Test
    public void isValidEvent_valid() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper();

        assertThat(StatisticValidator.validate(statName, statisticWrapper).value.isValid()).isTrue();
    }

    @Test
    public void isValidEvent_invalid_nameMismatch() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper();

        statisticWrapper.getStatistic().setName("OtherStatName");

        assertThat(StatisticValidator.validate(statName, statisticWrapper).value.isValid()).isFalse();
    }

    @Test
    public void isValidEvent_invalid_typeMismatch1() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper();

        statisticWrapper.getStatistic().setValue(123.456);
        statisticWrapper.getStatistic().setCount(null);

        assertThat(StatisticValidator.validate(statName, statisticWrapper).value.isValid()).isFalse();
    }

    @Test
    public void isValidEvent_invalid_typeMismatch2() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper();

        ((MockStatisticConfiguration) statisticWrapper.getOptionalStatisticConfiguration().get()).setStatisticType(StatisticType.VALUE);

        assertThat(StatisticValidator.validate(statName, statisticWrapper).value.isValid()).isFalse();
    }

    @Test
    public void isValidEvent_invalid_missingStatConfig() throws Exception {

        StatisticWrapper statisticWrapper = buildBasicStatWrapper();

        statisticWrapper = new StatisticWrapper(statisticWrapper.getStatistic(), Optional.empty());

        assertThat(StatisticValidator.validate(statName, statisticWrapper).value.isValid()).isFalse();
    }

    private StatisticWrapper buildBasicStatWrapper() throws DatatypeConfigurationException {
        ZonedDateTime time = ZonedDateTime.of(
                LocalDateTime.of(2017, 2, 27, 10, 50, 30),
                ZoneOffset.UTC);
        String tag1 = "tag1";
        String tag2 = "tag2";
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        StatisticType statisticType = StatisticType.COUNT;

        Statistics.Statistic statistic = StatisticsHelper.buildCountStatistic(statName, time, 1L,
                StatisticsHelper.buildTagType(tag1, tag1 + "val1"),
                StatisticsHelper.buildTagType(tag2, tag2 + "val1")
        );

        MockStatisticConfiguration statConfig = new MockStatisticConfiguration()
                .setName(statName)
                .setStatisticType(statisticType)
                .setRollUpType(StatisticRollUpType.ALL)
                .addFieldName(tag1)
                .addFieldName(tag2)
                .setPrecision(interval);

        return new StatisticWrapper(statistic, Optional.of(statConfig));
    }

}