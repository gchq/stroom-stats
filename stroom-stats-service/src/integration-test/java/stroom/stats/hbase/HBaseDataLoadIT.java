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
import stroom.stats.api.StatisticTag;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Load data into HBase via the {@link StatisticsService} and then query it via the {@link StatisticsService}
 */
public class HBaseDataLoadIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDataLoadIT.class);

    Injector injector = getApp().getInjector();
    UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);
    StatisticConfigurationService statisticConfigurationService = injector.getInstance(StatisticConfigurationService.class);
    SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
    StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller = injector.getInstance(StatisticConfigurationEntityMarshaller.class);

    @Test
    public void testCount() {
        StatisticsService statisticsService = injector.getInstance(StatisticsService.class);
        UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);

        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        long timeMs1 = interval.truncateTimeToColumnInterval(ZonedDateTime.now().toInstant().toEpochMilli());
        long timeMs2 = interval.truncateTimeToColumnInterval(Instant.ofEpochMilli(timeMs1).plus(1, ChronoUnit.MINUTES).toEpochMilli());
        LOGGER.info("Time: {}", ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeMs1), ZoneOffset.UTC));
        List<AggregatedEvent> aggregatedEvents = new ArrayList<>();

        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString();

        String tag1Str = "tag1";
        String tag1Val1Str = tag1Str + "val1";
        String tag2Str = "tag2";
        String tag2Val1Str = tag2Str + "val1";
        String tag2Val2Str = tag2Str + "val2";

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
        UID tag2val2 = uniqueIdCache.getOrCreateId(tag2Val2Str);

        StatKey statKey1 = new StatKey( statName,
                rollUpBitMask,
                interval,
                timeMs1,
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val1));

        long statValue1 = 100L;

        StatAggregate statAggregate1 = new CountAggregate(statValue1);

        AggregatedEvent aggregatedEvent1 = new AggregatedEvent(statKey1, statAggregate1);

        //Add two of the same event so hbase will aggregate the count
        aggregatedEvents.add(aggregatedEvent1);
        aggregatedEvents.add(aggregatedEvent1);

        StatKey statKey2 = new StatKey( statName,
                rollUpBitMask,
                interval,
                timeMs2,
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val2));

        long statValue2 = 200L;

        StatAggregate statAggregate2 = new CountAggregate(statValue2);

        AggregatedEvent aggregatedEvent2 = new AggregatedEvent(statKey2, statAggregate2);

        //Add two of the same event so hbase will aggregate the count
        aggregatedEvents.add(aggregatedEvent2);
        aggregatedEvents.add(aggregatedEvent2);

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
                tag2Str,
                ExpressionTerm.Condition.EQUALS,
                tag2Val1Str);

        ExpressionItem tag2TermNotFound = new ExpressionTerm(
                tag2Str,
                ExpressionTerm.Condition.EQUALS,
                "ValueThatDoesn'tExist");

        Query queryAllData = new Query(
                new DocRef(StatisticConfigurationEntity.ENTITY_TYPE, statisticConfigurationEntity.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm));

        Query querySpecificRow = new Query(
                new DocRef(StatisticConfigurationEntity.ENTITY_TYPE, statisticConfigurationEntity.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, tag1Term, tag2Term));

        Query queryNoDataFound = new Query(
                new DocRef(StatisticConfigurationEntity.ENTITY_TYPE, statisticConfigurationEntity.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, tag1Term, tag2TermNotFound));

        List<StatisticDataPoint> dataPoints1 = runQuery(statisticsService, queryAllData, statisticConfigurationEntity, 2);

        //should have 3 distinct tag values as t1 is same for btoh and t2 is different for each
        assertThat(dataPoints1.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str, tag2Val2Str);
        assertThat(dataPoints1.stream().map(StatisticDataPoint::getCount).mapToLong(Long::longValue).sum()).isEqualTo((statValue1 * 2) + (statValue2 * 2));

        List<StatisticDataPoint> dataPoints2 = runQuery(statisticsService, querySpecificRow, statisticConfigurationEntity, 1);

        //shoudl only get two distinct tag values as we have just one row back
        assertThat(dataPoints2.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str);
        assertThat(dataPoints2.stream().map(StatisticDataPoint::getCount).mapToLong(Long::longValue).sum()).isEqualTo(statValue1 * 2);

        List<StatisticDataPoint> dataPoints3 = runQuery(statisticsService, queryNoDataFound, statisticConfigurationEntity, 0);
    }


    private List<StatisticDataPoint> runQuery(
            StatisticsService statisticsService,
            Query query,
            StatisticConfigurationEntity statisticConfigurationEntity,
            int expectedRecCount) {
        StatisticDataSet statisticDataSet = statisticsService.searchStatisticsData(query, statisticConfigurationEntity);

        assertThat(statisticDataSet).isNotNull();
        assertThat(statisticDataSet).size().isEqualTo(expectedRecCount);
        return statisticDataSet.getStatisticDataPoints();
    }





}
