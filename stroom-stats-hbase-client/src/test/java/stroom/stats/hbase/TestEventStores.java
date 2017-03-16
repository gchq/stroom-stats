

/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.hbase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.common.CommonStatisticConstants;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.aggregator.EventStoresPutAggregator;
import stroom.stats.hbase.structure.AddEventOperation;
import stroom.stats.hbase.table.*;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.server.common.AbstractStatisticsService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class TestEventStores {
    private EventStores eventStores;

    @Mock
    private EventStoresPutAggregator mockEventStoresPutAggregator;

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

        eventStores = new EventStores(new MockRowKeyCache(), new MockUniqueIdCache(), mockEventStoresPutAggregator,
                mockTableFactory, mockPropertyService);
    }

    @Test
    public void testPutEventInsidePurgeRetention() {
        final long timeOneMinAgo = System.currentTimeMillis() - DateUtil.MIN_MS;

        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(timeOneMinAgo);

        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());

        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
                statisticTypeCaptor.capture());

        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();

        Assert.assertEquals(1, opsList.size());

        final AddEventOperation addEventOperation = opsList.get(0);

        // operation comes through with the same interval
        Assert.assertEquals(EventStoreTimeIntervalEnum.HOUR, addEventOperation.getTimeInterval());
    }

    @Test
    public void testPutEventoutsideFirstPurgeRetention() {
        final long timeOneMinAgo = System.currentTimeMillis() - (EventStoreTimeIntervalEnum.HOUR.rowKeyInterval() * 2);

        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(timeOneMinAgo);

        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());

        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
                statisticTypeCaptor.capture());

        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();

        Assert.assertEquals(1, opsList.size());

        final AddEventOperation addEventOperation = opsList.get(0);

        // operation comes through with the next biggest interval as the event
        // is outside the purge interval for the
        // minute store
        Assert.assertEquals(EventStoreTimeIntervalEnum.DAY, addEventOperation.getTimeInterval());
    }

    @Test
    public void testPutEventoutsideAllPurgeRetention() {
        final long timeOneMinAgo = System.currentTimeMillis() - (EventStoreTimeIntervalEnum.DAY.rowKeyInterval() * 2);

        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(timeOneMinAgo);

        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());

        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
                statisticTypeCaptor.capture());

        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();

        // no operations come through as the event is outside the purge
        // retention for all stores
        Assert.assertEquals(0, opsList.size());

    }

    @Test
    public void testPutSingleEvent() {
        final StatisticTag tag1 = new StatisticTag("T1", "T1V1");
        final StatisticTag tag2 = new StatisticTag("T2", "T2V2");

        final RolledUpStatisticEvent rolledUpStatisticEvent = buildEvent(System.currentTimeMillis(),
                Arrays.asList(tag1, tag2));

        eventStores.putEvent(rolledUpStatisticEvent, EventStoreTimeIntervalEnum.HOUR.columnInterval());

        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
                statisticTypeCaptor.capture());

        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();

        // 4 ops, spawned from one event because of rollups.
        Assert.assertEquals(4, opsList.size());
    }

    @Test
    public void testPutTwoEvents() {
        final StatisticTag tag1 = new StatisticTag("T1", "T1V1");
        final StatisticTag tag2 = new StatisticTag("T2", "T2V2");

        final RolledUpStatisticEvent rolledUpStatisticEvent1 = buildEvent(System.currentTimeMillis(),
                Arrays.asList(tag1, tag2));
        final RolledUpStatisticEvent rolledUpStatisticEvent2 = buildEvent(System.currentTimeMillis(),
                Arrays.asList(tag1, tag2));

        eventStores.putEvents(Arrays.asList(rolledUpStatisticEvent1, rolledUpStatisticEvent2),
                EventStoreTimeIntervalEnum.HOUR.columnInterval(), StatisticType.COUNT);

        Mockito.verify(mockEventStoresPutAggregator, Mockito.times(1)).putEvents(addEventOperationCaptor.capture(),
                statisticTypeCaptor.capture());

        final List<AddEventOperation> opsList = addEventOperationCaptor.getValue();

        // 8 ops, spawned from 2 events because of rollups.
        Assert.assertEquals((4 * 2), opsList.size());
    }

    private RolledUpStatisticEvent buildEvent(final long timeMs) {
        return buildEvent(timeMs, null);
    }

    private RolledUpStatisticEvent buildEvent(final long timeMs, final List<StatisticTag> tags) {
        final StatisticEvent statisticEvent = new StatisticEvent(timeMs, "MyStat", tags, 1);

        final MockStatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();

        statisticConfiguration.setRollUpType(StatisticRollUpType.ALL);

        final RolledUpStatisticEvent rolledUpStatisticEvent = AbstractStatisticsService.generateTagRollUps(statisticEvent,
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
