

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

package stroom.stats.hbase.aggregator;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Test;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticType;
import stroom.stats.common.CommonStatisticConstants;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.structure.AddEventOperation;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ColumnQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public class TestEventStoresPutAggregatorImpl {
    MockStroomPropertyService propertyService = new MockStroomPropertyService();

    UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    private static final double JUNIT_DOUBLE_DELTA = 0.00001;

    @Test
    public void testAllSameEventNameAndTimeCount() {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        // 10 stats per store
        setProperties(10, 1000000000, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        CellQualifier lastCellQualifierUsed = null;

        final List<AddEventOperation> operations = new ArrayList<>();

        for (int i = 1; i <= 15; i++) {
            final String eventTimeString = "2009-01-01T10:11:12.134Z";
            final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 100L);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            lastCellQualifierUsed = cellQualifiers.get(0);

            operations.add(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent));
        }

        putAggregator.putEvents(operations, StatisticType.COUNT);

        assertEquals(0, threadAggregatorMock.getStatTypeList().size());

        // flush will flush minute store, roll up into hour store, flush that,
        // then roll up into day store and flush
        // that
        putAggregator.flushAll();

        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        assertEquals(3, stores.size());
        assertEquals(3, statTypes.size());

        InMemoryEventStoreCount countStore = (InMemoryEventStoreCount) stores.get(0);

        // only one thing in the map as the name and time is the same for all
        // events
        assertEquals(1, countStore.getSize());
        assertEquals(workingInterval, countStore.getTimeInterval());
        assertEquals(lastCellQualifierUsed.getRowKey(), countStore.iterator().next().getKey());
        assertEquals(lastCellQualifierUsed.getColumnQualifier(),
                countStore.iterator().next().getValue().keySet().iterator().next());

        assertEquals(1500L, countStore.iterator().next().getValue().values().iterator().next().longValue());

        countStore = (InMemoryEventStoreCount) stores.get(1);

        // only one thing in the map as the name and time is the same for all
        // events
        assertEquals(1, countStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get(), countStore.getTimeInterval());
        assertEquals(1500L, countStore.iterator().next().getValue().values().iterator().next().longValue());

        countStore = (InMemoryEventStoreCount) stores.get(2);

        // only one thing in the map as the name and time is the same for all
        // events
        assertEquals(1, countStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(
                EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get(), countStore.getTimeInterval());
        assertEquals(1500L, countStore.iterator().next().getValue().values().iterator().next().longValue());

        assertEquals(StatisticType.COUNT, statTypes.get(0));

    }

    @Test
    public void testAllSameEventNameAndTimeValue() {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        // 10 stats per store
        setProperties(10, 1000000000, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        CellQualifier lastCellQualifierUsed = null;

        final List<AddEventOperation> operations = new ArrayList<>();

        for (int i = 1; i <= 15; i++) {
            final String eventTimeString = "2009-01-01T10:11:12.134Z";
            final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 0.1);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);
            lastCellQualifierUsed = cellQualifiers.get(0);

            operations.add(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent));

        }

        putAggregator.putEvents(operations, StatisticType.VALUE);

        assertEquals(0, threadAggregatorMock.getStatTypeList().size());

        // flush will flush minute store, roll up into hour store, flush that,
        // then roll up into day store and flush
        // that
        putAggregator.flushAll();

        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        assertEquals(3, stores.size());
        assertEquals(3, statTypes.size());

        InMemoryEventStoreValue valueStore = (InMemoryEventStoreValue) stores.get(0);

        // only one thing in the map as the name and time is the same for all
        // events
        assertEquals(1, valueStore.getSize());
        assertEquals(workingInterval, valueStore.getTimeInterval());
        assertEquals(lastCellQualifierUsed, valueStore.iterator().next().getKey());
        assertEquals(1.5, valueStore.iterator().next().getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);

        valueStore = (InMemoryEventStoreValue) stores.get(1);

        // only one thing in the map as the name and time is the same for all
        // events
        assertEquals(1, valueStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get(), valueStore.getTimeInterval());
        assertEquals(1.5, valueStore.iterator().next().getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);

        valueStore = (InMemoryEventStoreValue) stores.get(2);

        // only one thing in the map as the name and time is the same for all
        // events
        assertEquals(1, valueStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(
                EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get(), valueStore.getTimeInterval());
        assertEquals(1.5, valueStore.iterator().next().getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);

        assertEquals(StatisticType.VALUE, statTypes.get(0));

    }

    /**
     * puts 15 events each one 60s apart. Should flush the minute store after 10
     * with the forced flush giving 5 more
     */
    @Test
    public void testAllSameEventNameDifferentTimesFlushOnFullCount() {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        setProperties(10, 1000000000, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final String eventTimeString = "2009-01-01T10:11:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        final List<AddEventOperation> operations = new ArrayList<>();

        for (int i = 1; i <= 15; i++) {
            // add 60s to the time
            eventTime += 60_000;

            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            operations.add(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent));

        }

        putAggregator.putEvents(operations, StatisticType.COUNT);

        assertEquals(1, threadAggregatorMock.getCallCount());
        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        InMemoryEventStoreCount countStore = (InMemoryEventStoreCount) stores.get(0);
        assertEquals(10, countStore.getSize());
        assertEquals(workingInterval, countStore.getTimeInterval());

        assertAllValuesInCountStore(countStore, 1L);

        // flush will flush the second minute store, roll up into hour store,
        // flush that, then roll up into day store
        // and flush
        // that
        putAggregator.flushAll();

        // two minute stores and, one hour and one day
        assertEquals(4, threadAggregatorMock.getCallCount());

        // get 2nd minute store
        countStore = (InMemoryEventStoreCount) stores.get(1);
        assertEquals(5, countStore.getSize());
        assertEquals(workingInterval, countStore.getTimeInterval());

        assertAllValuesInCountStore(countStore, 1L);

        // get hour store - only one value in map as all times are in same hour
        countStore = (InMemoryEventStoreCount) stores.get(2);
        assertEquals(1, countStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get(), countStore.getTimeInterval());
        assertEquals(15L, countStore.iterator().next().getValue().values().iterator().next().longValue());

        // get the day store
        countStore = (InMemoryEventStoreCount) stores.get(3);
        assertEquals(1, countStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(
                EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get(), countStore.getTimeInterval());
        assertEquals(15L, countStore.iterator().next().getValue().values().iterator().next().longValue());

        assertEquals(StatisticType.COUNT, statTypes.get(0));

    }

    /**
     * puts 15 events each one 60s apart. Should flush the minute store after 10
     * with the forced flush giving 5 more
     */
    @Test
    public void testAllSameEventNameDifferentTimesFlushOnFullValue() {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        setProperties(10, 1000000000, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final String eventTimeString = "2009-01-01T10:11:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        final List<AddEventOperation> operations = new ArrayList<>();

        for (int i = 1; i <= 15; i++) {
            // add 60s to the time
            eventTime += 60_000;

            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 0.1);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            operations.add(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent));

        }

        putAggregator.putEvents(operations, StatisticType.VALUE);

        assertEquals(1, threadAggregatorMock.getCallCount());
        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        InMemoryEventStoreValue valueStore = (InMemoryEventStoreValue) stores.get(0);
        assertEquals(10, valueStore.getSize());
        assertEquals(workingInterval, valueStore.getTimeInterval());

        // all values in the map must be one
        for (final Entry<CellQualifier, ValueCellValue> entry : valueStore) {
            assertEquals(0.1, entry.getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);
        }

        // flush will flush the second minute store, roll up into hour store,
        // flush that, then roll up into day store
        // and flush
        // that
        putAggregator.flushAll();

        // two minute stores and, one hour and one day
        assertEquals(4, threadAggregatorMock.getCallCount());

        // get 2nd minute store
        valueStore = (InMemoryEventStoreValue) stores.get(1);
        assertEquals(5, valueStore.getSize());
        assertEquals(workingInterval, valueStore.getTimeInterval());

        // all values in the map must be one
        for (final Entry<CellQualifier, ValueCellValue> entry : valueStore) {
            assertEquals(0.1, entry.getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);
        }

        // get hour store - only one value in map as all times are in same hour
        valueStore = (InMemoryEventStoreValue) stores.get(2);
        assertEquals(1, valueStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get(), valueStore.getTimeInterval());
        assertEquals(1.5, valueStore.iterator().next().getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);

        // get the day store
        valueStore = (InMemoryEventStoreValue) stores.get(3);
        assertEquals(1, valueStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(
                EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get(), valueStore.getTimeInterval());
        assertEquals(1.5, valueStore.iterator().next().getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);

        assertEquals(StatisticType.VALUE, statTypes.get(0));

    }

    /**
     * Put an event for each second in a 2 day period and check that the correct
     * number go into the second, minute, hour and day stores
     */
    @Test
    public void testAllSameEventNameWithRollUpCount() {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.SECOND;

        // use event store size so it will not flush until forced
        setProperties(1_000_000, 1000000000, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        // loop for number of seconds in 2 days
        final int eventCount = 60 * 60 * 24 * 2;

        final List<AddEventOperation> operations = new ArrayList<>();

        for (int i = 1; i <= eventCount; i++) {
            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            operations.add(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent));

            if (i % 1000 == 0) {
                putAggregator.putEvents(operations, StatisticType.COUNT);
                operations.clear();
            }

            // add 1s to the time
            eventTime += 1_000;
        }
        putAggregator.putEvents(operations, StatisticType.COUNT);

        assertEquals(0, threadAggregatorMock.getCallCount());

        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        InMemoryEventStoreCount countStore;

        // flush will flush the second minute store, roll up into hour store,
        // flush that, then roll up into day store
        // and flush
        // that
        putAggregator.flushAll();

        // one of each store
        assertEquals(4, threadAggregatorMock.getCallCount());

        // get second store
        countStore = (InMemoryEventStoreCount) stores.get(0);
        assertEquals(eventCount, countStore.getSize());
        assertEquals(workingInterval, countStore.getTimeInterval());

        assertAllValuesInCountStore(countStore, 1L);

        // get minute store
        countStore = (InMemoryEventStoreCount) stores.get(1);
        assertEquals((eventCount / 60), countStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get(), countStore.getTimeInterval());

        assertAllValuesInCountStore(countStore, 60L);

        // get hour store - only one value in map as all times are in same hour
        countStore = (InMemoryEventStoreCount) stores.get(2);
        assertEquals((eventCount / 60 / 60), countStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(
                EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get(), countStore.getTimeInterval());

        assertAllValuesInCountStore(countStore, (60L * 60L));

        // get the day store
        countStore = (InMemoryEventStoreCount) stores.get(3);
        assertEquals((eventCount / 60 / 60 / 24), countStore.getSize());
        assertEquals(
                EventStoreTimeIntervalHelper.getNextBiggest(EventStoreTimeIntervalHelper
                        .getNextBiggest(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get()).get(),
                countStore.getTimeInterval());

        assertAllValuesInCountStore(countStore, (60L * 60L * 24L));

        assertEquals(StatisticType.COUNT, statTypes.get(0));

    }

    /**
     * Put an event for each second in a 2 day period and check that the correct
     * number go into the second, minute, hour and day stores
     */
    @Test
    public void testAllSameEventNameWithRollUpValue() {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.SECOND;

        // use event store size so it will not flush until forced
        setProperties(1_000_000, 1000000000, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        // loop for number of seconds in 2 days
        final int eventCount = 60 * 60 * 24 * 2;

        final List<AddEventOperation> operations = new ArrayList<>(eventCount);

        for (int i = 1; i <= eventCount; i++) {
            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 0.1);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            operations.add(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent));

            if (i % 1000 == 0) {
                putAggregator.putEvents(operations, StatisticType.VALUE);
                operations.clear();
            }

            // add 1s to the time
            eventTime += 1_000;
        }

        putAggregator.putEvents(operations, StatisticType.VALUE);

        assertEquals(0, threadAggregatorMock.getCallCount());

        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        InMemoryEventStoreValue valueStore;

        // flush will flush the second minute store, roll up into hour store,
        // flush that, then roll up into day store
        // and flush
        // that
        putAggregator.flushAll();

        // one of each store
        assertEquals(4, threadAggregatorMock.getCallCount());

        // get second store
        valueStore = (InMemoryEventStoreValue) stores.get(0);
        assertEquals(eventCount, valueStore.getSize());
        assertEquals(workingInterval, valueStore.getTimeInterval());

        // all values in the map must be one
        for (final Entry<CellQualifier, ValueCellValue> entry : valueStore) {
            assertEquals(0.1, entry.getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);
        }

        // get minute store
        valueStore = (InMemoryEventStoreValue) stores.get(1);
        assertEquals((eventCount / 60), valueStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get(), valueStore.getTimeInterval());

        // all values in the map must be 60, i.e. 60s in a minute
        for (final Entry<CellQualifier, ValueCellValue> entry : valueStore) {
            assertEquals(60 * 0.1, entry.getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);
        }

        // get hour store - only one value in map as all times are in same hour
        valueStore = (InMemoryEventStoreValue) stores.get(2);
        assertEquals((eventCount / 60 / 60), valueStore.getSize());
        assertEquals(EventStoreTimeIntervalHelper.getNextBiggest(
                EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get(), valueStore.getTimeInterval());

        // all values in the map must be 3600, ie. 3600s in an hour
        for (final Entry<CellQualifier, ValueCellValue> entry : valueStore) {
            assertEquals((60 * 60 * 0.1), entry.getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);
        }

        // get the day store
        valueStore = (InMemoryEventStoreValue) stores.get(3);
        assertEquals((eventCount / 60 / 60 / 24), valueStore.getSize());
        assertEquals(
                EventStoreTimeIntervalHelper.getNextBiggest(EventStoreTimeIntervalHelper
                        .getNextBiggest(EventStoreTimeIntervalHelper.getNextBiggest(workingInterval).get()).get()).get(),
                valueStore.getTimeInterval());

        // all values in the map must be 86,400, ie. 86,400s in a day
        for (final Entry<CellQualifier, ValueCellValue> entry : valueStore) {
            assertEquals((60 * 60 * 24 * 0.1), entry.getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);
        }

        assertEquals(StatisticType.VALUE, statTypes.get(0));

    }

    /**
     * Put a single event with a big store size and low timeout and verify that
     * the event is flushed when the timout has passed and execut is called.
     *
     * @throws InterruptedException
     */
    @Test
    public void testTimeoutCount() throws InterruptedException {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.SECOND;

        final long timeoutMillis = 2_000L;

        // big store, 5s timeout
        setProperties(1_000_000, timeoutMillis, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

        final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

        final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

        putAggregator.putEvents(
                Arrays.asList(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent)),
                StatisticType.COUNT);

        // store not full and not yet timed out
        assertEquals(0, threadAggregatorMock.getCallCount());

        // simulate the method called by cron each minute to check the delay
        // queue
        ((EventStoresPutAggregatorImpl) putAggregator).flushQueue();

        // store still not timed out so should be ignored
        assertEquals(0, threadAggregatorMock.getCallCount());

        Thread.sleep(timeoutMillis + 100);

        // simulate the method called by cron each minute to check the delay
        // queue
        ((EventStoresPutAggregatorImpl) putAggregator).flushQueue();

        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        InMemoryEventStoreCount countStore;

        // get second store
        countStore = (InMemoryEventStoreCount) stores.get(0);
        assertEquals(1, countStore.getSize());
        assertEquals(workingInterval, countStore.getTimeInterval());
        assertEquals(1L, countStore.iterator().next().getValue().values().iterator().next().longValue());

        assertEquals(StatisticType.COUNT, statTypes.get(0));

    }

    /**
     * Put a single event with a big store size and low timeout and verify that
     * the event is flushed when the timout has passed and execut is called.
     *
     * @throws InterruptedException
     */
    @Test
    public void testTimeoutValue() throws InterruptedException {
        // set up the properties we need

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.SECOND;

        final long timeoutMillis = 2_000L;

        // big store, 5s timeout
        setProperties(1_000_000, timeoutMillis, 2);

        // mock the threadFlush aggregator that the put aggregator will call
        // Tried to do this with Mockito but for some reason it doesn't capture
        // the right number of calls
        final MockEventStoresThreadFlushAggregator threadAggregatorMock = new MockEventStoresThreadFlushAggregator();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        // initialise the class under test with the mock services
        final EventStoresPutAggregator putAggregator = new EventStoresPutAggregatorImpl(threadAggregatorMock,
                propertyService, eventStoreIdPool);

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 0.1);

        final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

        final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

        putAggregator.putEvents(
                Arrays.asList(new AddEventOperation(workingInterval, cellQualifiers.get(0), rolledUpStatisticEvent)),
                StatisticType.VALUE);

        // store not full and not yet timed out
        assertEquals(0, threadAggregatorMock.getCallCount());

        // simulate the method called by cron each minute to check the delay
        // queue
        ((EventStoresPutAggregatorImpl) putAggregator).flushQueue();

        // store still not timed out so should be ignored
        assertEquals(0, threadAggregatorMock.getCallCount());

        Thread.sleep(timeoutMillis + 500);

        // simulate the method called by cron each minute to check the delay
        // queue
        ((EventStoresPutAggregatorImpl) putAggregator).flushQueue();

        final List<AbstractInMemoryEventStore> stores = threadAggregatorMock.getStoreList();
        final List<StatisticType> statTypes = threadAggregatorMock.getStatTypeList();

        InMemoryEventStoreValue valueStore;

        // get second store
        valueStore = (InMemoryEventStoreValue) stores.get(0);
        assertEquals(1, valueStore.getSize());
        assertEquals(workingInterval, valueStore.getTimeInterval());
        assertEquals(0.1, valueStore.iterator().next().getValue().getAggregatedValue(), JUNIT_DOUBLE_DELTA);

        assertEquals(StatisticType.VALUE, statTypes.get(0));

    }

    private void setProperties(final int maxSize, final long timeOutMillis, final int poolSize) {
        propertyService.setProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME,
                Integer.toString(maxSize));

        for (final EventStoreTimeIntervalEnum timeInterval : EventStoreTimeIntervalEnum.values()) {
            propertyService
                    .setProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME_PREFIX
                            + timeInterval.longName().toLowerCase(), Long.toString(timeOutMillis));
        }

        propertyService.setProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX
                + StatisticType.COUNT.name().toLowerCase(), Integer.toString(poolSize));

        propertyService.setProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX
                + StatisticType.VALUE.name().toLowerCase(), Integer.toString(poolSize));

        propertyService.setProperty(CommonStatisticConstants.STROOM_STATISTIC_ENGINES_PROPERTY_NAME, "hbase");

    }

    private void assertAllValuesInCountStore(final InMemoryEventStoreCount countStore, final long expectedValue) {
        for (final Entry<RowKey, Map<ColumnQualifier, MutableLong>> rowEntry : countStore) {
            for (final Entry<ColumnQualifier, MutableLong> cellEntry : rowEntry.getValue().entrySet()) {
                assertEquals(expectedValue, cellEntry.getValue().longValue());
            }
        }
    }
}
