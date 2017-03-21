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

package stroom.stats.hbase;

import com.google.inject.Injector;
import org.hibernate.SessionFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.DocRef;
import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.Query;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticConfigurationEntityHelper;
import stroom.stats.util.DateUtil;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class HBaseDataLoadIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDataLoadIT.class);

    Injector injector = getApp().getInjector();
    UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);
    StatisticConfigurationService statisticConfigurationService = injector.getInstance(StatisticConfigurationService.class);
    SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
    StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller = injector.getInstance(StatisticConfigurationEntityMarshaller.class);

    @Test
    public void test() {
        StatisticsService statisticsService = injector.getInstance(StatisticsService.class);
        UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);

        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        long timeMs = interval.truncateTimeToColumnInterval(ZonedDateTime.now().toInstant().toEpochMilli());
        LOGGER.info("Time: {}", ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeMs), ZoneOffset.UTC));
        List<AggregatedEvent> aggregatedEvents = new ArrayList<>();

        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString();
        String tag1Str = "tag1";
        String tag1Val1Str = tag1Str + "val1";
        String tag2Str = "tag2";
        String tag2Val1Str = tag2Str + "val1";

        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                statNameStr,
                statisticType,
                interval.columnInterval(),
                StatisticRollUpType.ALL)
                .addFields(tag1Str, tag2Str)
                .build();

        StatisticConfigurationEntityHelper.addStatConfig(
                sessionFactory,
                statisticConfigurationEntityMarshaller,
                statisticConfigurationEntity);

        UID statName = uniqueIdCache.getOrCreateId(statNameStr);
        assertThat(statName).isNotNull();

        UID tag1 = uniqueIdCache.getOrCreateId(tag1Str);
        UID tag1val1 = uniqueIdCache.getOrCreateId(tag1Val1Str);
        UID tag2 = uniqueIdCache.getOrCreateId(tag2Str);
        UID tag2val1 = uniqueIdCache.getOrCreateId(tag2Val1Str);

        StatKey statKey = new StatKey( statName,
                rollUpBitMask,
                interval,
                timeMs,
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val1));

        long statValue = 100L;

        StatAggregate statAggregate = new CountAggregate(statValue);

        AggregatedEvent aggregatedEvent = new AggregatedEvent(statKey, statAggregate);

        aggregatedEvents.add(aggregatedEvent);

        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);

        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.BETWEEN,
                String.format("%s,%s",
                        DateUtil.createNormalDateTimeString(Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli()),
                        DateUtil.createNormalDateTimeString(Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli())));

        ExpressionItem tag1Term = new ExpressionTerm(
                tag1Str,
                ExpressionTerm.Condition.EQUALS,
                tag1Val1Str);

        ExpressionItem tag2Term = new ExpressionTerm(
                tag1Str,
                ExpressionTerm.Condition.EQUALS,
                tag2Val1Str);

        Query query = new Query(
                new DocRef(StatisticConfigurationEntity.ENTITY_TYPE, statisticConfigurationEntity.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, tag1Term, tag2Term));

        StatisticDataSet statisticDataSet = statisticsService.searchStatisticsData(query, statisticConfigurationEntity);

        assertThat(statisticDataSet).isNotNull();
        assertThat(statisticDataSet).size().isEqualTo(1);
        StatisticDataPoint statisticDataPoint = statisticDataSet.getStatisticDataPoints().stream().findFirst().get();
        assertThat(statisticDataPoint.getCount()).isEqualTo(statValue);
        assertThat(statisticDataPoint.getTags()).size().isEqualTo(statKey.getTagValues().size());
    }





}
