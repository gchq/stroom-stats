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
import javaslang.Tuple2;
import org.hibernate.SessionFactory;
import org.junit.Test;
import stroom.query.api.v2.DocRef;
import stroom.query.api.v2.ExpressionItem;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.Field;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.SearchRequest;
import stroom.query.api.v2.SearchResponse;
import stroom.stats.AbstractAppIT;
import stroom.stats.HBaseClient;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.CountStatisticDataPoint;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.Period;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.ValueStatisticDataPoint;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.test.QueryApiHelper;
import stroom.stats.test.StatisticsHelper;
import stroom.stats.test.StroomStatsStoreEntityBuilder;
import stroom.stats.test.StroomStatsStoreEntityHelper;
import stroom.stats.util.DateUtil;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Load data into HBase via the {@link StatisticsService} and then query it via the {@link StatisticsService}
 */
public class HBaseDataLoadIT extends AbstractAppIT {

    private Injector injector = getApp().getInjector();
    private UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);
    private StatisticsService statisticsService = injector.getInstance(StatisticsService.class);
    private HBaseClient hBaseClient = injector.getInstance(HBaseClient.class);
    private SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
    private StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller = injector.getInstance(StroomStatsStoreEntityMarshaller.class);

    private EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.DAY;
    private ChronoUnit workingChronoUnit = ChronoUnit.DAYS;
    private static final RollUpBitMask ROLL_UP_BIT_MASK = RollUpBitMask.ZERO_MASK;
    private static int MAX_EVENT_IDS = 100;

    private final Instant time1 = ZonedDateTime.now().toInstant();
    private final Instant time2 = time1.plus(5, workingChronoUnit);

    //effectively time1, time1+5d, time1+10d, time1+15d, time1+20d
    private List<Instant> times = IntStream.rangeClosed(0, 4)
            .boxed()
            .map(i -> time1.plus(5 * i, workingChronoUnit))
            .collect(Collectors.toList());

    private List<Instant> timesTruncated = times.stream()
            .map(time -> time.truncatedTo(workingChronoUnit))
            .collect(Collectors.toList());

    private String tag1Str = "tag1";
    private String tag1Val1Str = tag1Str + "val1";
    private String tag2Str = "tag2";
    private String tag2Val1Str = tag2Str + "val1";
    private String tag2Val2Str = tag2Str + "val2";

    private UID tag1 = uniqueIdCache.getOrCreateId(tag1Str);
    private UID tag1val1 = uniqueIdCache.getOrCreateId(tag1Val1Str);
    private UID tag2 = uniqueIdCache.getOrCreateId(tag2Str);
    private UID tag2val1 = uniqueIdCache.getOrCreateId(tag2Val1Str);
    private UID tag2val2 = uniqueIdCache.getOrCreateId(tag2Val2Str);
    private UID rolledUpValue = uniqueIdCache.getOrCreateId(RollUpBitMask.ROLL_UP_TAG_VALUE);

//    private FilterTermsTree.TermNode dateTerm = new FilterTermsTree.TermNode(
//            StatisticConfiguration.FIELD_NAME_DATE_TIME,
//            ExpressionTerm.Condition.BETWEEN,
//            String.format("%s,%s",
//                    DateUtil.createNormalDateTimeString(Instant.now().minus(10, workingChronoUnit).toEpochMilli()),
//                    DateUtil.createNormalDateTimeString(Instant.now().plus(10, workingChronoUnit).toEpochMilli())));

    private ExpressionTerm precisionTerm = new ExpressionTerm(
            StatisticConfiguration.FIELD_NAME_PRECISION,
            ExpressionTerm.Condition.EQUALS,
            interval.toString().toLowerCase());

    private FilterTermsTree.TermNode tag1Term = new FilterTermsTree.TermNode(
            tag1Str,
            FilterTermsTree.Condition.EQUALS,
            tag1Val1Str);

    private FilterTermsTree.TermNode tag2Term = new FilterTermsTree.TermNode(
            tag2Str,
            FilterTermsTree.Condition.EQUALS,
            tag2Val1Str);

    private FilterTermsTree.TermNode tag2TermNotFound = new FilterTermsTree.TermNode(
            tag2Str,
            FilterTermsTree.Condition.EQUALS,
            "ValueThatDoesn'tExist");

    /**
     * Load two COUNT stats then query using a variety of query terms to filter the data
     */
    @Test
    public void testCount() {

        StatisticType statisticType = StatisticType.COUNT;
        Map<StatEventKey, StatAggregate> aggregatedEvents = new HashMap<>();

        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString();
        String statUuidStr = StatisticsHelper.getUuidKey(statNameStr);

        StatisticConfiguration statisticConfigurationEntity = new StroomStatsStoreEntityBuilder(
                statUuidStr,
                statNameStr,
                statisticType,
                interval,
                StatisticRollUpType.ALL)
                .addFields(tag1Str, tag2Str)
                .build();

        StroomStatsStoreEntityHelper.addStatConfig(
                sessionFactory,
                stroomStatsStoreEntityMarshaller,
                statisticConfigurationEntity);

        UID statUuidUid = uniqueIdCache.getOrCreateId(statUuidStr);
        assertThat(statUuidUid).isNotNull();

        StatEventKey statEventKey1 = new StatEventKey(statUuidUid,
                ROLL_UP_BIT_MASK,
                interval,
                time1.toEpochMilli(),
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val1));

        long statValue1 = 100L;

        StatAggregate statAggregate1 = new CountAggregate(statValue1);

        aggregatedEvents.put(statEventKey1, statAggregate1);

        StatEventKey statEventKey2 = new StatEventKey(statUuidUid,
                ROLL_UP_BIT_MASK,
                interval,
                time2.toEpochMilli(),
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val2));

        long statValue2 = 200L;

        StatAggregate statAggregate2 = new CountAggregate(statValue2);

        aggregatedEvents.put(statEventKey2, statAggregate2);

        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);

        SearchStatisticsCriteria criteriaAllData = buildSearchStatisticsCriteria(statUuidStr,
                statisticConfigurationEntity);

        SearchStatisticsCriteria criteriaSpecificRow = buildSearchStatisticsCriteria(statUuidStr,
                statisticConfigurationEntity,
                tag1Term,
                tag2Term);

        SearchStatisticsCriteria criteriaNoDataFound = buildSearchStatisticsCriteria(statUuidStr,
                statisticConfigurationEntity,
                tag1Term,
                tag2TermNotFound);

        List<StatisticDataPoint> dataPoints = runCriteria(statisticsService, criteriaAllData, statisticConfigurationEntity, 2);

        //should have 3 distinct tag values as t1 is same for btoh and t2 is different for each
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str, tag2Val2Str);
        assertThat(computeSumOfCountCounts(dataPoints)).isEqualTo((statValue1) + (statValue2));

        dataPoints = runCriteria(statisticsService, criteriaSpecificRow, statisticConfigurationEntity, 1);

        //should only get two distinct tag values as we have just one row back
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str);
        assertThat(computeSumOfCountCounts(dataPoints)).isEqualTo(statValue1);

        //should get nothing back as the requested tagvalue is not in the store
        runCriteria(statisticsService, criteriaNoDataFound, statisticConfigurationEntity, 0);

        //No put the same events again so the values should be aggregated by HBase
        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);

        dataPoints = runCriteria(statisticsService, criteriaAllData, statisticConfigurationEntity, 2);

        //should have 3 distinct tag values as t1 is same for btoh and t2 is different for each
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str, tag2Val2Str);
        assertThat(computeSumOfCountCounts(dataPoints)).isEqualTo((statValue1 * 2) + (statValue2 * 2));

        dataPoints = runCriteria(statisticsService, criteriaSpecificRow, statisticConfigurationEntity, 1);

        //should only get two distinct tag values as we have just one row back
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str);
        assertThat(computeSumOfCountCounts(dataPoints)).isEqualTo(statValue1 * 2);

        //should get nothing back as the requested tagvalue is not in the store
        runCriteria(statisticsService, criteriaNoDataFound, statisticConfigurationEntity, 0);
    }

    private SearchStatisticsCriteria buildSearchStatisticsCriteria(String statUuidStr,
                                                                   StatisticConfiguration statisticConfiguration,
                                                                   FilterTermsTree.Node... childNodes) {

        SearchStatisticsCriteria.SearchStatisticsCriteriaBuilder builder = SearchStatisticsCriteria
                .builder(new Period(), statUuidStr)
                .setInterval(interval)
                .setRequiredDynamicFields(statisticConfiguration.getFieldNames());
        if (childNodes != null && childNodes.length > 0) {
            builder.setFilterTermsTree(new FilterTermsTree(new OperatorNode(
                    FilterTermsTree.Operator.AND, childNodes)));
        }
        return builder.build();
    }

    /**
     * Load two VALUE stats then query using a variety of query terms to filter the data
     */
    @Test
    public void testValue() {

        StatisticType statisticType = StatisticType.VALUE;

        Map<StatEventKey, StatAggregate> aggregatedEvents = new HashMap<>();

        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString();
        String statUuidStr = StatisticsHelper.getUuidKey(statNameStr);

        StatisticConfiguration statisticConfiguration = new StroomStatsStoreEntityBuilder(
                statUuidStr,
                statNameStr,
                statisticType,
                interval,
                StatisticRollUpType.ALL)
                .addFields(tag1Str, tag2Str)
                .build();

        StroomStatsStoreEntityHelper.addStatConfig(
                sessionFactory,
                stroomStatsStoreEntityMarshaller,
                statisticConfiguration);

        UID statUuidUid = uniqueIdCache.getOrCreateId(statUuidStr);
        assertThat(statUuidStr).isNotNull();

        StatEventKey statEventKey1 = new StatEventKey(statUuidUid,
                ROLL_UP_BIT_MASK,
                interval,
                time1.toEpochMilli(),
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val1));

        double statValue1 = 1.23;

        StatAggregate statAggregate1 = new ValueAggregate(statValue1);

        aggregatedEvents.put(statEventKey1, statAggregate1);

        StatEventKey statEventKey2 = new StatEventKey(statUuidUid,
                ROLL_UP_BIT_MASK,
                interval,
                time2.toEpochMilli(),
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val2));

        double statValue2 = 2.34;

        StatAggregate statAggregate2 = new ValueAggregate(statValue2);

        aggregatedEvents.put(statEventKey2, statAggregate2);

        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);

        SearchStatisticsCriteria criteriaAllData = buildSearchStatisticsCriteria(statUuidStr,
                statisticConfiguration);

        SearchStatisticsCriteria criteriaSpecificRow = buildSearchStatisticsCriteria(statUuidStr,
                statisticConfiguration,
                tag1Term,
                tag2Term);

        SearchStatisticsCriteria criteriaNoDataFound = buildSearchStatisticsCriteria(statUuidStr,
                statisticConfiguration,
                tag1Term,
                tag2TermNotFound);

        List<StatisticDataPoint> dataPoints = runCriteria(statisticsService, criteriaAllData, statisticConfiguration, 2);

        //should have 3 distinct tag values as t1 is same for btoh and t2 is different for each
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str, tag2Val2Str);
        assertThat(computeSumOfValueValues(dataPoints)).isEqualTo((statValue1) + (statValue2));

        dataPoints = runCriteria(statisticsService, criteriaSpecificRow, statisticConfiguration, 1);

        //should only get two distinct tag values as we have just one row back
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str);
        assertThat(computeSumOfValueValues(dataPoints)).isEqualTo(statValue1);

        //should get nothing back as the requested tagvalue is not in the store
        runCriteria(statisticsService, criteriaNoDataFound, statisticConfiguration, 0);

        //now put the same events again so HBase should aggregated the values in the cells
        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);

        dataPoints = runCriteria(statisticsService, criteriaAllData, statisticConfiguration, 2);

        //should have 3 distinct tag values as t1 is same for btoh and t2 is different for each
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str, tag2Val2Str);
        assertThat(computeSumOfValueValues(dataPoints)).isEqualTo((statValue1) + (statValue2));
        assertThat(computeSumOfValueCounts(dataPoints)).isEqualTo(2 + 2);

        dataPoints = runCriteria(statisticsService, criteriaSpecificRow, statisticConfiguration, 1);

        //should only get two distinct tag values as we have just one row back
        assertThat(dataPoints.stream().flatMap(dataPoint -> dataPoint.getTags().stream()).map(StatisticTag::getValue).distinct().collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(tag1Val1Str, tag2Val1Str);
        assertThat(computeSumOfValueValues(dataPoints)).isEqualTo(statValue1);
        assertThat(computeSumOfValueCounts(dataPoints)).isEqualTo(2);

        //should get nothing back as the requested tagvalue is not in the store
        runCriteria(statisticsService, criteriaNoDataFound, statisticConfiguration, 0);
    }

    @Test
    public void testDateHandling_missingDateTerm() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, precisionTerm));

        //all records should come back
        runQuery(statisticsService, wrapQuery(query, statisticConfiguration), statisticConfiguration, times.size());
    }

    @Test
    public void testRollUpSelection_noFilterTerms_noTagsInTable() {

        Set<RollUpBitMask> rollUpBitMasks = RollUpBitMask.getRollUpBitMasks(2);

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT, rollUpBitMasks);

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, precisionTerm));

        //only put the static fields in the table settings so we will expect it to roll up all the tags
        SearchRequest searchRequest = QueryApiHelper.wrapQuery(query, StatisticConfiguration.STATIC_FIELDS_MAP.get(StatisticType.COUNT));

        //all records should come back
        SearchResponse searchResponse = runQuery(statisticsService, searchRequest, statisticConfiguration, times.size());

        List<String> fields = searchRequest.getResultRequests().get(0).getMappings().get(0).getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toList());

        assertThat(fields).doesNotContain(
                statisticConfiguration.getFieldNames().toArray(
                        new String[statisticConfiguration.getFieldNames().size()]));
    }

    @Test
    public void testDateHandling_equals() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        //use timesTruncated as that is how they are stored in hbase
        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.EQUALS,
                DateUtil.createNormalDateTimeString(timesTruncated.get(2).toEpochMilli()));

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, precisionTerm));

        SearchRequest searchRequest = wrapQuery(query, statisticConfiguration);

        //only find the the one we equal
        SearchResponse searchResponse = runQuery(statisticsService, searchRequest, statisticConfiguration, 1);

        List<Instant> times = getInstants(searchResponse);

        assertThat(times.get(0)).isEqualTo(timesTruncated.get(2));
    }


    @Test
    public void testDateHandling_lessThan() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        //use timesTruncated as that is how they are stored in hbase
        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.LESS_THAN,
                DateUtil.createNormalDateTimeString(timesTruncated.get(2).toEpochMilli()));

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, precisionTerm));

        SearchRequest searchRequest = wrapQuery(query, statisticConfiguration);

        //should only get the two times below the middle one we have aimed for
        SearchResponse searchResponse = runQuery(statisticsService, searchRequest, statisticConfiguration, 2);

        List<Instant> times = getInstants(searchResponse);

        assertThat(times).containsExactlyInAnyOrder(timesTruncated.get(0), timesTruncated.get(1));
    }

    @Test
    public void testDateHandling_lessThanEqualTo() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        //use timesTruncated as that is how they are stored in hbase
        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO,
                DateUtil.createNormalDateTimeString(timesTruncated.get(2).toEpochMilli()));

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, precisionTerm));

        SearchRequest searchRequest = wrapQuery(query, statisticConfiguration);

        //should only get the two times below the middle one we have aimed for
        SearchResponse searchResponse = runQuery(statisticsService, wrapQuery(query, statisticConfiguration), statisticConfiguration, 3);
        List<Instant> times = getInstants(searchResponse);

        assertThat(times).containsExactlyInAnyOrder(timesTruncated.get(0), timesTruncated.get(1), timesTruncated.get(2));
    }

    @Test
    public void testDateHandling_greaterThan() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        //truncate the original stat time down to the interval so we can find it with an EQUALS
        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.GREATER_THAN,
                DateUtil.createNormalDateTimeString(timesTruncated.get(2).toEpochMilli()));

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, precisionTerm));

        SearchRequest searchRequest = wrapQuery(query, statisticConfiguration);

        //should only get the two times below the middle one we have aimed for
        SearchResponse searchResponse = runQuery(statisticsService, searchRequest, statisticConfiguration, 2);
        List<Instant> times = getInstants(searchResponse);
        assertThat(times).containsExactlyInAnyOrder(timesTruncated.get(3), timesTruncated.get(4));
    }

    @Test
    public void testDateHandling_greaterThanEqualTo() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        //use timesTruncated as that is how they are stored in hbase
        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO,
                DateUtil.createNormalDateTimeString(timesTruncated.get(2).toEpochMilli()));

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, precisionTerm));

        SearchRequest searchRequest = wrapQuery(query, statisticConfiguration);

        //should only get the two times below the middle one we have aimed for
        SearchResponse searchResponse = runQuery(statisticsService, wrapQuery(query, statisticConfiguration), statisticConfiguration, 3);

        List<Instant> times = getInstants(searchResponse);

        assertThat(times).containsExactlyInAnyOrder(timesTruncated.get(2), timesTruncated.get(3), timesTruncated.get(4));
    }

    @Test
    public void testDateHandling_between() {

        StatisticConfiguration statisticConfiguration = loadStatData(StatisticType.COUNT);

        //use timesTruncated as that is how they are stored in hbase
        ExpressionItem dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.BETWEEN,
                String.format("%s,%s",
                        DateUtil.createNormalDateTimeString(timesTruncated.get(1).toEpochMilli()),
                        DateUtil.createNormalDateTimeString(timesTruncated.get(3).toEpochMilli())));

        Query query = new Query(
                new DocRef(StatisticConfiguration.ENTITY_TYPE, statisticConfiguration.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateTerm, precisionTerm));

        SearchRequest searchRequest = wrapQuery(query, statisticConfiguration);

        //should only get the two times below the middle one we have aimed for
        SearchResponse searchResponse = runQuery(statisticsService, wrapQuery(query, statisticConfiguration), statisticConfiguration, 3);
        List<Instant> times = getInstants(searchResponse);
        assertThat(times).containsExactlyInAnyOrder(timesTruncated.get(1), timesTruncated.get(2), timesTruncated.get(3));
    }

    private StatisticConfiguration createStatConf(final String statUuid,
                                                  final String statNameStr,
                                                  final StatisticType statisticType) {

        StatisticConfiguration statisticConfiguration = new StroomStatsStoreEntityBuilder(
                statUuid,
                statNameStr,
                statisticType,
                interval,
                StatisticRollUpType.ALL)
                .addFields(tag1Str, tag2Str)
                .build();

        StroomStatsStoreEntityHelper.addStatConfig(
                sessionFactory,
                stroomStatsStoreEntityMarshaller,
                statisticConfiguration);

        return statisticConfiguration;
    }

    private StatisticConfiguration loadStatData(final StatisticType statisticType) {
        return loadStatData(statisticType, Collections.singleton(ROLL_UP_BIT_MASK));
    }

    private StatisticConfiguration loadStatData(final StatisticType statisticType, Set<RollUpBitMask> rollUpBitMasks) {
        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameBase = this.getClass().getName() + "-test-" + Instant.now().toString();
        List<StatisticConfiguration> entities = new ArrayList<>();

        //create stats for multiple stat names to make sure we are not picking up other records
        Arrays.asList("A", "B", "C").forEach(postFix -> {

            String statNameStr = statNameBase + "-" + postFix;
            String statUuidStr = StatisticsHelper.getUuidKey(statNameStr);
            StatisticConfiguration statisticConfiguration = createStatConf(statUuidStr, statNameStr, statisticType);
            entities.add(statisticConfiguration);

            UID statUuidUid = uniqueIdCache.getOrCreateId(statUuidStr);
            assertThat(statUuidUid).isNotNull();

            Map<StatEventKey, StatAggregate> aggregatedEvents = times.stream()
                    .flatMap(time -> {
                        StatEventKey baseKey = new StatEventKey(statUuidUid,
                                RollUpBitMask.ZERO_MASK,
                                interval,
                                time.toEpochMilli(),
                                new TagValue(tag1, tag1val1),
                                new TagValue(tag2, tag2val1));

                        return rollUpBitMasks.stream()
                                .map(newMask -> {
                                    StatEventKey statEventKey = baseKey.cloneAndRollUpTags(newMask, rolledUpValue);

                                    StatAggregate statAggregate = new CountAggregate(100L);
                                    return new Tuple2<>(statEventKey, statAggregate);
                                });
                    })
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, StatAggregate::aggregatePair));

            assertThat(aggregatedEvents).hasSize(times.size() * rollUpBitMasks.size());

            statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);
        });

        //return the middle entity
        return entities.get(1);
    }

    private List<StatisticDataPoint> runCriteria(
            StatisticsService statisticsService,
            SearchStatisticsCriteria searchStatisticsCriteria,
            StatisticConfiguration statisticConfiguration,
            int expectedRecCount) {
        StatisticDataSet statisticDataSet = statisticsService.searchStatisticsData(searchStatisticsCriteria, statisticConfiguration);

        assertThat(statisticDataSet).isNotNull();
        assertThat(statisticDataSet).size().isEqualTo(expectedRecCount);
        return statisticDataSet.getStatisticDataPoints();
    }

    private SearchResponse runQuery(
            StatisticsService statisticsService,
            SearchRequest searchRequest,
            StatisticConfiguration statisticConfiguration,
            int expectedRecCount) {

        SearchResponse searchResponse = hBaseClient.query(searchRequest);

        assertThat(searchResponse).isNotNull();
        int rowCount = QueryApiHelper.getFlatResult(searchResponse)
                .map(flatResult -> flatResult.getValues().size())
                .orElse(0);
        assertThat(rowCount).isEqualTo(expectedRecCount);
        return searchResponse;
    }

    private SearchRequest wrapQuery(Query query, StatisticConfiguration statisticConfiguration) {
        return QueryApiHelper.wrapQuery(query, statisticConfiguration.getAllFieldNames());
    }

    private static long computeSumOfCountCounts(List<StatisticDataPoint> dataPoints) {

        return dataPoints.stream()
                .map(point -> (CountStatisticDataPoint) point)
                .map(CountStatisticDataPoint::getCount)
                .mapToLong(Long::longValue)
                .sum();
    }

    private static long computeSumOfValueCounts(List<StatisticDataPoint> dataPoints) {

        return dataPoints.stream()
                .map(point -> (ValueStatisticDataPoint) point)
                .map(ValueStatisticDataPoint::getCount)
                .mapToLong(Long::longValue)
                .sum();
    }

    private static double computeSumOfValueValues(List<StatisticDataPoint> dataPoints) {

        return dataPoints.stream()
                .map(point -> (ValueStatisticDataPoint) point)
                .map(ValueStatisticDataPoint::getValue)
                .mapToDouble(Double::doubleValue)
                .sum();
    }

    private List<Instant> getInstants(final SearchResponse searchResponse) {
        return QueryApiHelper.getFlatResult(searchResponse)
                .map(flatResult ->
                        QueryApiHelper.getTypedFieldValues(
                                flatResult,
                                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                                Instant.class))
                .orElseGet(Collections::emptyList);
    }
}
