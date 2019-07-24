

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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import stroom.stats.common.CommonStatisticConstants;
import stroom.stats.common.Period;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.table.EventStoreTable;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TestEventStores {
    private EventStores eventStores;



    private final MockStroomPropertyService mockPropertyService = new MockStroomPropertyService();

    @Mock
    private EventStoreTableFactory mockTableFactory;

    private final Map<EventStoreTimeIntervalEnum, EventStoreTable> eventStoreTableMap = new EnumMap<>(EventStoreTimeIntervalEnum.class);

    private int maxIntervalsInPeriod = 5;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        // mockEventStoresPutAggregator =
        // Mockito.mock(EventStoresPutAggregator.class);

        mockPropertyService
                .setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                        + EventStoreTimeIntervalEnum.SECOND.name().toLowerCase(), "2");
        mockPropertyService
                .setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                        + EventStoreTimeIntervalEnum.MINUTE.name().toLowerCase(), "2");
        mockPropertyService
                .setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                        + EventStoreTimeIntervalEnum.HOUR.name().toLowerCase(), "2");
        mockPropertyService
                .setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                        + EventStoreTimeIntervalEnum.DAY.name().toLowerCase(), "2");

        mockPropertyService
                .setProperty(HBaseStatisticConstants.SEARCH_MAX_INTERVALS_IN_PERIOD_PROPERTY_NAME, maxIntervalsInPeriod);

        mockPropertyService.setProperty(CommonStatisticConstants.STROOM_STATISTIC_ENGINES_PROPERTY_NAME,
                HBaseStatisticsService.ENGINE_NAME);

        // eventStores = new EventStores(mockCacheManager, new
        // MockUniqueIdCacheFactory(), mockEventStoresPutAggregator,
        // mockTableFactory, mockPropertyService, null,
        // mockEventStoreScheduler);

        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            EventStoreTable eventStoreTable = Mockito.mock(EventStoreTable.class, interval.longName());
            eventStoreTableMap.put(interval, eventStoreTable);

            Mockito.when(mockTableFactory.getEventStoreTable(Mockito.eq(interval)))
                    .thenReturn(eventStoreTable);
        }

        eventStores = new EventStores(new MockUniqueIdCache(), mockTableFactory, mockPropertyService);
    }

    @Test
    public void testGetStatisticsData_noPeriod() {
        EventStoreTimeIntervalEnum finestInterval = EventStoreTimeIntervalEnum.MINUTE;
        EventStoreTimeIntervalEnum expectedInterval = EventStoreTimeIntervalEnum.FOREVER;

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(
                Period.createNullPeriod(), "myuuid")
                .build();

        eventStores.getStatisticsData(criteria, buildStatisticConfiguration(finestInterval));

        assertIntervalQueried(expectedInterval);
    }

    @Test
    public void testGetStatisticsData_forcePrecision() {

        EventStoreTimeIntervalEnum forcedInterval = EventStoreTimeIntervalEnum.HOUR;
        EventStoreTimeIntervalEnum finestInterval = EventStoreTimeIntervalEnum.MINUTE;
        EventStoreTimeIntervalEnum expectedInterval = forcedInterval;

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(
                Period.createNullPeriod(), "myuuid")
                .setInterval(forcedInterval)
                .build();

        eventStores.getStatisticsData(criteria, buildStatisticConfiguration(finestInterval));

        assertIntervalQueried(expectedInterval);
    }

    @Test
    public void testGetStatisticsData_tinyPeriod() {

        EventStoreTimeIntervalEnum finestInterval = EventStoreTimeIntervalEnum.SECOND;
        EventStoreTimeIntervalEnum expectedInterval = EventStoreTimeIntervalEnum.SECOND;

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(
                new Period(Instant.now().minus(2, ChronoUnit.SECONDS).toEpochMilli(),
                        Instant.now().toEpochMilli()), "myuuid")
                .build();

        eventStores.getStatisticsData(criteria, buildStatisticConfiguration(finestInterval));

        assertIntervalQueried(expectedInterval);
    }

    @Test
    public void testGetStatisticsData_tinyPeriodBumpedUpToFinestInterval() {

        // stat config is a min of MIN, so
        EventStoreTimeIntervalEnum finestInterval = EventStoreTimeIntervalEnum.MINUTE;
        EventStoreTimeIntervalEnum expectedInterval = EventStoreTimeIntervalEnum.MINUTE;

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(
                new Period(Instant.now().minus(2, ChronoUnit.SECONDS).toEpochMilli(),
                        Instant.now().toEpochMilli()), "myuuid")
                .build();

        eventStores.getStatisticsData(criteria, buildStatisticConfiguration(finestInterval));

        assertIntervalQueried(expectedInterval);
    }

    @Test
    public void testGetStatisticsData_tinyPeriodOutsidePurgeRetention() {

        // Period is v small but it is a long way in the past so out side purge retention
        // for SECOND
        EventStoreTimeIntervalEnum finestInterval = EventStoreTimeIntervalEnum.SECOND;
        EventStoreTimeIntervalEnum expectedInterval = EventStoreTimeIntervalEnum.MINUTE;

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(
                new Period(
                        Instant.now()
                                .minus(2, ChronoUnit.SECONDS)
                                .minusMillis(EventStoreTimeIntervalEnum.SECOND.rowKeyInterval() * 3)
                                .toEpochMilli(),
                        Instant.now()
                                .minusMillis(EventStoreTimeIntervalEnum.SECOND.rowKeyInterval() * 3)
                                .toEpochMilli()), "myuuid")
                .build();

        eventStores.getStatisticsData(criteria, buildStatisticConfiguration(finestInterval));

        assertIntervalQueried(expectedInterval);
    }

    @Test
    public void testGetStatisticsData_bigPeriod() {

        EventStoreTimeIntervalEnum finestInterval = EventStoreTimeIntervalEnum.SECOND;
        EventStoreTimeIntervalEnum expectedInterval = EventStoreTimeIntervalEnum.DAY;

        SearchStatisticsCriteria criteria = SearchStatisticsCriteria.builder(
                new Period(Instant.now().minusMillis(EventStoreTimeIntervalEnum.HOUR.columnInterval() * (maxIntervalsInPeriod + 1)).toEpochMilli(),
                        Instant.now().toEpochMilli()), "myuuid")
                .build();

        eventStores.getStatisticsData(criteria, buildStatisticConfiguration(finestInterval));

        assertIntervalQueried(expectedInterval);
    }

    private StatisticConfiguration buildStatisticConfiguration(final EventStoreTimeIntervalEnum finestINterval) {

        StatisticConfiguration statisticConfiguration = Mockito.mock(StatisticConfiguration.class);
        Mockito.when(statisticConfiguration.getPrecision())
                .thenReturn(finestINterval);
        Mockito.when(statisticConfiguration.getRollUpType())
                .thenReturn(StatisticRollUpType.NONE);
        return statisticConfiguration;
    }


    private void assertIntervalQueried(final EventStoreTimeIntervalEnum expectedInterval) {
        EventStoreTable expectedEventStoreTable = eventStoreTableMap.get(expectedInterval);

        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            if (!interval.equals(expectedInterval)) {
                EventStoreTable eventStoreTable = eventStoreTableMap.get(interval);

                Mockito.verify(eventStoreTable, Mockito.never()).getStatisticsData(
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any(),
                        Mockito.any());
            }
        }

        // Now override the verify for the one we expect
        Mockito.verify(expectedEventStoreTable).getStatisticsData(
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any());
    }

    @Test
    public void testPutEventInsidePurgeRetention() {
        //TODO migrate this logic to a test of the flatmap processor
//        final long timeOneMinAgo = System.currentTimeMillis() - DateUtil.MIN_MS;
//
//        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(timeOneMinAgo);
//
//        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());
//
//        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
//                statisticTypeCaptor.capture());
//
//        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();
//
//        Assert.assertEquals(1, opsList.size());
//
//        final AddEventOperation addEventOperation = opsList.get(0);
//
//        // operation comes through with the same interval
//        Assert.assertEquals(EventStoreTimeIntervalEnum.HOUR, addEventOperation.getTimeInterval());
    }

    @Test
    public void testPutEventoutsideFirstPurgeRetention() {
        //TODO migrate this logic to a test of the flatmap processor
//        final long timeOneMinAgo = System.currentTimeMillis() - (EventStoreTimeIntervalEnum.HOUR.rowKeyInterval() * 2);
//
//        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(timeOneMinAgo);
//
//        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());
//
//        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
//                statisticTypeCaptor.capture());
//
//        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();
//
//        Assert.assertEquals(1, opsList.size());
//
//        final AddEventOperation addEventOperation = opsList.get(0);
//
//        // operation comes through with the next biggest interval as the event
//        // is outside the purge interval for the
//        // minute store
//        Assert.assertEquals(EventStoreTimeIntervalEnum.DAY, addEventOperation.getTimeInterval());
    }

    @Test
    public void testPutEventoutsideAllPurgeRetention() {
        //TODO migrate this logic to a test of the flatmap processor
//        final long timeOneMinAgo = System.currentTimeMillis() - (EventStoreTimeIntervalEnum.DAY.rowKeyInterval() * 2);
//
//        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(timeOneMinAgo);
//
//        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());
//
//        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
//                statisticTypeCaptor.capture());
//
//        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();
//
//        // no operations come through as the event is outside the purge
//        // retention for all stores
//        Assert.assertEquals(0, opsList.size());

    }


//    private RolledUpStatisticEvent buildEvent(final long timeMs) {
//        return buildEvent(timeMs, null);
//    }
//
//    private RolledUpStatisticEvent buildEvent(final long timeMs, final List<StatisticTag> tags) {
//        final StatisticEvent statisticEvent = new StatisticEvent(timeMs, "MyStat", tags, 1);
//
//        final MockStatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();
//
//        statisticConfiguration.setRollUpType(StatisticRollUpType.ALL);
//
//        final RolledUpStatisticEvent rolledUpStatisticEvent = HBaseStatisticsService.generateTagRollUps(statisticEvent,
//                statisticConfiguration);
//
//        return rolledUpStatisticEvent;
//    }


}
