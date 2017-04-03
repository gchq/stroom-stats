

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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import stroom.stats.api.StatisticType;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.Period;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.*;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.table.EventStoreTable;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.DateUtil;

import java.util.*;

public class TestEventStore {
    EventStoreForTesting eventStore;
    MockEventStoreTable mockEventStoreTable;
    MockEventStoreTableFactory mockTableFactory;

    private static final String STAT_NAME = "MyStatName";
    private static final String TAG0 = "tag0";
    private static final String TAG1 = "tag1";
    private static final String TAG2 = "tag2";
    private static final String TAG3 = "tag3";

    private static final StatisticConfiguration STAT_CONFIG_ROLLUP_NONE = new MockStatisticConfiguration(
            STAT_NAME,
            StatisticType.COUNT,
            StatisticRollUpType.NONE,
            1000L,
            Collections.emptySet());

    private static final StatisticConfiguration STAT_CONFIG_ROLLUP_ALL = new MockStatisticConfiguration(
                STAT_NAME,
                StatisticType.COUNT,
                StatisticRollUpType.ALL,
                1000L,
                Collections.emptySet(),
                TAG0,
                TAG1,
                TAG2,
                TAG3);

    private static final StatisticConfiguration STAT_CONFIG_ROLLUP_CUSTOM = new MockStatisticConfiguration(
            STAT_NAME,
            StatisticType.COUNT,
            StatisticRollUpType.CUSTOM,
            1000L,
            new HashSet<>(Arrays.asList(
                    new MockCustomRollupMask(Collections.emptyList()), //zero mask
                    new MockCustomRollupMask(Arrays.asList(0, 1, 2, 3)), //all rolled up
                    new MockCustomRollupMask(Arrays.asList(0, 2)),
                    new MockCustomRollupMask(Arrays.asList(1, 3)),
                    new MockCustomRollupMask(Arrays.asList(0, 1)),
                    new MockCustomRollupMask(Arrays.asList(2, 3)))),
            TAG0,
            TAG1,
            TAG2,
            TAG3);

    private static final FilterTermsTree FILTER_TREE_TAG1_TAG3 = new FilterTermsTree(
                        new OperatorNode(FilterTermsTree.Operator.AND,
                                new FilterTermsTree.TermNode(TAG1, FilterTermsTree.Condition.EQUALS, "someValue"),
                                new FilterTermsTree.TermNode(TAG3, FilterTermsTree.Condition.EQUALS, "someValue")));

    private static final FilterTermsTree FILTER_TREE_TAG1 = new FilterTermsTree(
            new OperatorNode(FilterTermsTree.Operator.AND,
                    new FilterTermsTree.TermNode(TAG1, FilterTermsTree.Condition.EQUALS, "someValue")));

    @Test
    public void testBuildRollUpBitMaskFromCriteria_noFilterTerms_noRequiredFields_allRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_ALL);

        //no filter terms and no required fields so we can roll everything up
        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(0,1,2,3));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_noFilterTerms_twoRequiredFields() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setRequiredDynamicFields(Arrays.asList(TAG0, TAG2))
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_ALL);

        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(1,3));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_twoFilterTerms_noRequiredFields_allRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setFilterTermsTree(FILTER_TREE_TAG1_TAG3)
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_ALL);

        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(0,2));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_twoFilterTerms_twoRequiredFields_allRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setFilterTermsTree(FILTER_TREE_TAG1_TAG3)
                .setRequiredDynamicFields(Arrays.asList(TAG0, TAG2))
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_ALL);

        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.ZERO_MASK;

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_oneFilterTerms_oneRequiredFields_allRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setFilterTermsTree(FILTER_TREE_TAG1)
                .setRequiredDynamicFields(Arrays.asList(TAG2))
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_ALL);

        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(0,3));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_noFilterTerms_noRequiredFields_customRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_CUSTOM);

        //no filter terms and no required fields so we can roll everything up
        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(0,1,2,3));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_twoFilterTerms_noRequiredFields_customRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setFilterTermsTree(FILTER_TREE_TAG1_TAG3)
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_CUSTOM);


        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(0,2));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_noFilterTerms_twoRequiredFields_customRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setRequiredDynamicFields(Arrays.asList(TAG0, TAG2))
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_CUSTOM);


        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.fromTagPositions(Arrays.asList(1,3));

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_twoFilterTerms_twoRequiredFields_customRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .setFilterTermsTree(FILTER_TREE_TAG1_TAG3)
                .setRequiredDynamicFields(Arrays.asList(TAG0, TAG2))
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_CUSTOM);


        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.ZERO_MASK;

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testBuildRollUpBitMaskFromCriteria_noFilterTerms_noRequiredFields_noRollups() {

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(new Period(), STAT_NAME)
                .build();

        RollUpBitMask rollUpBitMask = EventStore.buildRollUpBitMaskFromCriteria(criteria, STAT_CONFIG_ROLLUP_NONE);

        //stat has rollups set to NONE so will always be a zero mask
        RollUpBitMask expectedRollUpBitMask = RollUpBitMask.ZERO_MASK;

        Assertions.assertThat(rollUpBitMask).isEqualTo(expectedRollUpBitMask);
    }

    @Test
    public void testPurgeStatisticDataSourceDataOneDataSourceNoTagsSecondStore() {
        // 2 retained intervals of an hour each so should be two and bit hours
        // before the current time
        testPurgeStatisticDataSourceOneDataSourceNoTags(EventStoreTimeIntervalEnum.SECOND, "2015-05-12T12:00:00.000Z");

    }

    @Test
    public void testPurgeStatisticDataSourceDataOneDataSourceNoTagsMinuteStore() {
        // 2 retained intervals of a day each so should be two and bit days
        // before the current time
        testPurgeStatisticDataSourceOneDataSourceNoTags(EventStoreTimeIntervalEnum.MINUTE, "2015-05-10T00:00:00.000Z");

    }

    private void testPurgeStatisticDataSourceOneDataSourceNoTags(final EventStoreTimeIntervalEnum interval,
            final String expectedPurgeUpToTime) {
        buildEventStore(interval, "2015-05-12T14:01:01.000Z", 2);

        final List<StatisticConfiguration> statisticConfigurations = new ArrayList<>();

        final StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();
        final MockStatisticConfiguration mockStatisticConfiguration = (MockStatisticConfiguration) statisticConfiguration;

        mockStatisticConfiguration.setName("MyStat");
        mockStatisticConfiguration.setEngineName(HBaseStatisticsService.ENGINE_NAME);
        mockStatisticConfiguration.setRollUpType(StatisticRollUpType.ALL);

        statisticConfigurations.add(statisticConfiguration);

        eventStore.purgeStatisticDataSourceData(null, statisticConfigurations, PurgeMode.OUTSIDE_RETENTION);

        Assert.assertEquals(1, mockEventStoreTable.getPurgeArgs().size());

        final PurgeArgsObject purgeArgs = mockEventStoreTable.getPurgeArgs().get(0);

        Assert.assertEquals(RollUpBitMask.ZERO_MASK, purgeArgs.getRollUpBitMask());
        Assert.assertEquals(statisticConfiguration.getName(), purgeArgs.getStatisticName());

        Assert.assertEquals(expectedPurgeUpToTime, DateUtil.createNormalDateTimeString(purgeArgs.purgeUpToTimeMs));
    }

    @Test
    public void testPurgeStatisticDataSourceTwoDataSourcesNoTags() {
        buildEventStore(EventStoreTimeIntervalEnum.MINUTE, "2015-05-12T14:01:01.000Z", 2);

        final List<StatisticConfiguration> statisticConfigurations = new ArrayList<>();

        final StatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();
        final MockStatisticConfiguration mockStatisticConfiguration = (MockStatisticConfiguration) statisticConfiguration;
        mockStatisticConfiguration.setName("MyStat1");
        mockStatisticConfiguration.setEngineName(HBaseStatisticsService.ENGINE_NAME);
        mockStatisticConfiguration.setRollUpType(StatisticRollUpType.ALL);
        statisticConfigurations.add(statisticConfiguration);

        final StatisticConfiguration statisticConfiguration2 = new MockStatisticConfiguration();
        final MockStatisticConfiguration mockStatisticConfiguration2 = (MockStatisticConfiguration) statisticConfiguration2;
        mockStatisticConfiguration2.setName("MyStat2");
        mockStatisticConfiguration2.setEngineName(HBaseStatisticsService.ENGINE_NAME);
        mockStatisticConfiguration2.setRollUpType(StatisticRollUpType.ALL);
        statisticConfigurations.add(statisticConfiguration2);

        eventStore.purgeStatisticDataSourceData(null, statisticConfigurations, PurgeMode.OUTSIDE_RETENTION);

        Assert.assertEquals(2, mockEventStoreTable.getPurgeArgs().size());

        final PurgeArgsObject purgeArgs1 = mockEventStoreTable.getPurgeArgs().get(0);
        final PurgeArgsObject purgeArgs2 = mockEventStoreTable.getPurgeArgs().get(1);

        Assert.assertEquals(RollUpBitMask.ZERO_MASK, purgeArgs1.getRollUpBitMask());
        Assert.assertEquals(statisticConfiguration.getName(), purgeArgs1.getStatisticName());

        Assert.assertEquals(RollUpBitMask.ZERO_MASK, purgeArgs2.getRollUpBitMask());
        Assert.assertEquals(statisticConfiguration2.getName(), purgeArgs2.getStatisticName());

        // 2 retained intervals of a day each so should be two and bit days
        // before the current time
        Assert.assertEquals("2015-05-10T00:00:00.000Z",
                DateUtil.createNormalDateTimeString(purgeArgs1.purgeUpToTimeMs));
        Assert.assertEquals(DateUtil.createNormalDateTimeString(purgeArgs1.purgeUpToTimeMs),
                DateUtil.createNormalDateTimeString(purgeArgs2.purgeUpToTimeMs));
    }

    @Test
    public void testPurgeStatisticDataSourceTwoDataSourcesTwoTagsRollUpDisabledOnOne() {
        buildEventStore(EventStoreTimeIntervalEnum.MINUTE, "2015-05-12T14:01:01.000Z", 2);

        final List<StatisticConfiguration> statisticConfigurations = new ArrayList<>();

        final MockStatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();
        statisticConfiguration.setName("MyStat1");
        statisticConfiguration.setEngineName(HBaseStatisticsService.ENGINE_NAME);
        statisticConfiguration.setRollUpType(StatisticRollUpType.ALL);
        statisticConfiguration.addFieldName("tag1");
        statisticConfiguration.addFieldName("tag2");
        statisticConfigurations.add(statisticConfiguration);

        final MockStatisticConfiguration statisticConfiguration2 = new MockStatisticConfiguration();
        statisticConfiguration2.setName("MyStat2");
        statisticConfiguration2.setEngineName(HBaseStatisticsService.ENGINE_NAME);
        statisticConfiguration2.setRollUpType(StatisticRollUpType.NONE);
        statisticConfiguration2.addFieldName("tag3");
        statisticConfiguration2.addFieldName("tag4");
        statisticConfigurations.add(statisticConfiguration2);

        eventStore.purgeStatisticDataSourceData(null, statisticConfigurations, PurgeMode.OUTSIDE_RETENTION);

        // called 4 times for the sat with 2 tags (i.e. 4 perms) and once from
        // the one with rollups disabled
        Assert.assertEquals(5, mockEventStoreTable.getPurgeArgs().size());

        Assert.assertEquals(2, mockEventStoreTable.getNames().size());
        Assert.assertTrue(mockEventStoreTable.getNames().contains(statisticConfiguration.getName()));
        Assert.assertTrue(mockEventStoreTable.getNames().contains(statisticConfiguration2.getName()));

        Assert.assertEquals(4, mockEventStoreTable.getMasks().size());
        Assert.assertTrue(mockEventStoreTable.getMasks().contains(RollUpBitMask.ZERO_MASK));
        Assert.assertTrue(mockEventStoreTable.getMasks().contains(RollUpBitMask.fromTagPositions(Arrays.asList(0))));
        Assert.assertTrue(mockEventStoreTable.getMasks().contains(RollUpBitMask.fromTagPositions(Arrays.asList(1))));
        Assert.assertTrue(mockEventStoreTable.getMasks().contains(RollUpBitMask.fromTagPositions(Arrays.asList(0, 1))));

        Assert.assertEquals(1, mockEventStoreTable.getTimes().size());
        Assert.assertEquals("2015-05-10T00:00:00.000Z",
                DateUtil.createNormalDateTimeString(mockEventStoreTable.times.iterator().next()));
    }

    @Test
    public void testIsTimeInsidePurgeRetention() {
        buildEventStore(EventStoreTimeIntervalEnum.SECOND, "2015-05-12T14:01:01.000Z", 2);

        // retention of 2 (1 hour) intervals so anything > 12:00:00 is good

        Assert.assertTrue(
                eventStore.isTimeInsidePurgeRetention(DateUtil.parseNormalDateTimeString("2015-05-12T14:01:01.000Z")));

        Assert.assertFalse(
                eventStore.isTimeInsidePurgeRetention(DateUtil.parseNormalDateTimeString("2015-05-12T10:01:01.000Z")));

        Assert.assertFalse(
                eventStore.isTimeInsidePurgeRetention(DateUtil.parseNormalDateTimeString("2015-05-12T11:59:59.999Z")));

        Assert.assertTrue(
                eventStore.isTimeInsidePurgeRetention(DateUtil.parseNormalDateTimeString("2015-05-12T12:00:00.000Z")));

    }

    private void buildEventStore(final EventStoreTimeIntervalEnum interval, final String currentTime,
            final int retainedIntervalCount) {
        mockEventStoreTable = new MockEventStoreTable();
        mockTableFactory = new MockEventStoreTableFactory(mockEventStoreTable);

        final MockStroomPropertyService propertyService = new MockStroomPropertyService();
        propertyService.setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                + interval.longName().toLowerCase(), Integer.toString(retainedIntervalCount));

        eventStore = new EventStoreForTesting(null, interval, mockTableFactory, propertyService, currentTime);

    }

    /**
     * Sub class the class under test so we can hard code the current system
     * time for testing
     */
    private static class EventStoreForTesting extends EventStore {
        private final String now;

        public EventStoreForTesting(final UniqueIdCache uidCache, final EventStoreTimeIntervalEnum interval,
                                    final EventStoreTableFactory eventStoreTableFactory, final StroomPropertyService propertyService, final String currentTime) {
            super(uidCache, interval, eventStoreTableFactory, propertyService);
            this.now = currentTime;
        }

        @Override
        public long getCurrentTimeMs() {
            return DateUtil.parseNormalDateTimeString(now);
        }
    }

    private static class MockEventStoreTableFactory implements EventStoreTableFactory {
        private final EventStoreTable eventStoreTable;

        public MockEventStoreTableFactory(final EventStoreTable eventStoreTable) {
            this.eventStoreTable = eventStoreTable;
        }

        @Override
        public EventStoreTable getEventStoreTable(final EventStoreTimeIntervalEnum timeinterval) {
            return eventStoreTable;
        }
    }

    private static class PurgeArgsObject {
        private final String statisticName;
        private final RollUpBitMask rollUpBitMask;
        private final long purgeUpToTimeMs;

        public PurgeArgsObject(final String statisticName, final RollUpBitMask rollUpBitMask,
                final long purgeUpToTimeMs) {
            this.statisticName = statisticName;
            this.rollUpBitMask = rollUpBitMask;
            this.purgeUpToTimeMs = purgeUpToTimeMs;
        }

        public String getStatisticName() {
            return statisticName;
        }

        public RollUpBitMask getRollUpBitMask() {
            return rollUpBitMask;
        }

        public long getPurgeUpToTimeMs() {
            return purgeUpToTimeMs;
        }

    }

    private static class MockEventStoreTable implements EventStoreTable {
        private final List<PurgeArgsObject> purgeArgs = new ArrayList<>();
        private final Set<String> names = new HashSet<>();
        private final Set<RollUpBitMask> masks = new HashSet<>();
        private final Set<Long> times = new HashSet<>();

        public void clearLists() {
            purgeArgs.clear();
            names.clear();
            masks.clear();
            times.clear();

        }

        @Override
        public EventStoreTimeIntervalEnum getInterval() {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        public List<PurgeArgsObject> getPurgeArgs() {
            return purgeArgs;
        }

        public Set<String> getNames() {
            return names;
        }

        public Set<RollUpBitMask> getMasks() {
            return masks;
        }

        public Set<Long> getTimes() {
            return times;
        }

        @Override
        public void addAggregatedEvents(final StatisticType statisticType, final Map<StatKey, StatAggregate> aggregatedEvents) {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        @Override
        public String getDisplayName() {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        @Override
        public HBaseConnection getTableConfiguration() {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        @Override
        public String getNameAsString() {
            throw new UnsupportedOperationException("Not used by this mock");
        }

//        @Override
//        public void bufferedAddCount(final CountRowData countRowData, final boolean isForcedFlushToDisk) {
//            throw new UnsupportedOperationException("Not used by this mock");
//        }
//
//        @Override
//        public void addMultipleCounts(List<CountRowData> rowChanges) {
//            throw new UnsupportedOperationException("Not used by this mock");
//        }
//
//        @Override
//        public void addValue(final CellQualifier cellQualifier, final ValueCellValue valueCellValue) {
//            throw new UnsupportedOperationException("Not used by this mock");
//        }

        @Override
        public StatisticDataSet getStatisticsData(final UniqueIdCache uniqueIdCache,
                final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask,
                final SearchStatisticsCriteria criteria) {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        @Override
        public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                final StatisticConfiguration statisticConfiguration) {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        @Override
        public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask,
                final Period period) {
            throw new UnsupportedOperationException("Not used by this mock");
        }

        @Override
        public void purgeUntilTime(final UniqueIdCache uniqueIdCache, final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask, final long purgeUpToTimeMs) {
            final String statisticName = statisticConfiguration.getName();
            purgeArgs.add(new PurgeArgsObject(statisticName, rollUpBitMask, purgeUpToTimeMs));
            names.add(statisticName);
            masks.add(rollUpBitMask);
            times.add(purgeUpToTimeMs);

        }

        @Override
        public void purgeAll(final UniqueIdCache uniqueIdCache, final StatisticConfiguration statisticConfiguration) {
            purgeArgs.add(new PurgeArgsObject(statisticConfiguration.getName(), null, -1));
            names.add(statisticConfiguration.getName());
        }

//        @Override
//        public void flushPutBuffer() {
//            throw new UnsupportedOperationException("Not used by this mock");
//        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("Not used by this mock");
        }

//        @Override
//        public int getPutBufferCount(final boolean isDeepCount) {
//            throw new UnsupportedOperationException("Not used by this mock");
//        }
//
//        @Override
//        public int getAvailableBatchPutTaskPermits() {
//            throw new UnsupportedOperationException("Not used by this mock");
//        }

        @Override
        public long getCellsPutCount(final StatisticType statisticType) {
            throw new UnsupportedOperationException("Not used by this mock");
        }

    }
}
