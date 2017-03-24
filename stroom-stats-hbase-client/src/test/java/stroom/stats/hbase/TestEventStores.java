

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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.common.CommonStatisticConstants;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.structure.AddEventOperation;
import stroom.stats.hbase.table.EventStoreTable;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.io.IOException;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class TestEventStores {
    private EventStores eventStores;

    @Captor
    private ArgumentCaptor<List<AddEventOperation>> addEventOperationCaptor;

    @Captor
    private ArgumentCaptor<StatisticType> statisticTypeCaptor;

    private final MockStroomPropertyService mockPropertyService = new MockStroomPropertyService();

    private final MockEventStoreTableFactory mockTableFactory = new MockEventStoreTableFactory();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        // mockEventStoresPutAggregator =
        // Mockito.mock(EventStoresPutAggregator.class);

        mockPropertyService
                .setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                        + EventStoreTimeIntervalEnum.HOUR.name().toLowerCase(), "1");
        mockPropertyService
                .setProperty(HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                        + EventStoreTimeIntervalEnum.DAY.name().toLowerCase(), "1");

        mockPropertyService.setProperty(CommonStatisticConstants.STROOM_STATISTIC_ENGINES_PROPERTY_NAME,
                HBaseStatisticsService.ENGINE_NAME);

        // eventStores = new EventStores(mockCacheManager, new
        // MockUniqueIdCacheFactory(), mockEventStoresPutAggregator,
        // mockTableFactory, mockPropertyService, null,
        // mockEventStoreScheduler);

        eventStores = new EventStores(new MockRowKeyCache(), new MockUniqueIdCache(),
                mockTableFactory, mockPropertyService);
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


    private RolledUpStatisticEvent buildEvent(final long timeMs) {
        return buildEvent(timeMs, null);
    }

    private RolledUpStatisticEvent buildEvent(final long timeMs, final List<StatisticTag> tags) {
        final StatisticEvent statisticEvent = new StatisticEvent(timeMs, "MyStat", tags, 1);

        final MockStatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();

        statisticConfiguration.setRollUpType(StatisticRollUpType.ALL);

        final RolledUpStatisticEvent rolledUpStatisticEvent = HBaseStatisticsService.generateTagRollUps(statisticEvent,
                statisticConfiguration);

        return rolledUpStatisticEvent;
    }

    public static class MockEventStoreTableFactory implements EventStoreTableFactory {
        @Override
        public EventStoreTable getEventStoreTable(final EventStoreTimeIntervalEnum timeinterval) {
            return null;
        }
    }

}
