

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

package stroom.stats.hbase.aggregator;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Test;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticType;
import stroom.stats.common.CommonStatisticConstants;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.store.task.EventStoreFlushTask;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.task.api.AbstractTaskHandler;
import stroom.stats.task.api.Task;
import stroom.stats.task.api.TaskCallback;
import stroom.stats.task.api.TaskManager;
import stroom.stats.task.api.ThreadPool;
import stroom.stats.task.api.VoidResult;
import stroom.stats.util.DateUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestEventStoresThreadFlushAggregatorImpl {
    MockStroomPropertyService propertyService = new MockStroomPropertyService();
    MockTaskManager taskManager = new MockTaskManager();
    UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    /**
     * Very crude test just to exercise the constructor and make sure the
     * objects are all in place by immediately calling a flush
     */
    @Test
    public void testEventStoresThreadFlushAggregatorImpl() {
        setProperties(1_000_000, 60_000);

        taskManager.resetCallData();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        final EventStoresThreadFlushAggregator threadAggregator = new EventStoresThreadFlushAggregatorImpl(
                propertyService, taskManager, eventStoreIdPool);

        threadAggregator.flushAll();

        assertEquals(0, taskManager.getCallCount());

    }

    /**
     * Submits a store containing a small number of values so the aggregated
     * store will not fill up and thus will only flush when forced
     */
    @Test
    public void testAddFlushedStatisticStoreNotFull() {
        // big store, big timeout
        setProperties(1_000_000, 3_600_000);

        taskManager.resetCallData();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        final EventStoresThreadFlushAggregator threadAggregator = new EventStoresThreadFlushAggregatorImpl(
                propertyService, taskManager, eventStoreIdPool);

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final InMemoryEventStoreCount storeToFlush = new InMemoryEventStoreCount(
                getEventStoreMapKey(StatisticType.COUNT, workingInterval));

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        CellQualifier lastCellQualifierUsed = null;

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        for (int i = 1; i <= 15; i++) {
            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            lastCellQualifierUsed = cellQualifiers.get(0);

            storeToFlush.putValue(cellQualifiers.get(0), 2);

            eventTime += 60_000L;
        }

        // verify the content of the store before we add it
        assertEquals(15, storeToFlush.getSize());

        assertAllValuesInCountStore(storeToFlush, 2);

        // now pass our store to the aggregator
        threadAggregator.addFlushedStatistics(storeToFlush);

        assertEquals(0, taskManager.getCallCount());

        threadAggregator.flushAll();

        assertEquals(1, taskManager.getCallCount());

        final List<AbstractInMemoryEventStore> storeList = taskManager.getStoresSubmitted();

        final ConcurrentInMemoryEventStoreCount aggregatedStore = (ConcurrentInMemoryEventStoreCount) storeList.get(0);

        assertEquals(15, aggregatedStore.getSize());

        assertAllValuesInConcurrentCountStore(aggregatedStore, 2);

    }

    /**
     * Uses a small aggregate store size so the incoming data fills the store.
     * The remaining data will only get flushed when forced
     */
    @Test
    public void testAddFlushedStatisticsFillsUpStore() {
        // tiny store, big timeout
        setProperties(10, 3_600_000);

        taskManager.resetCallData();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        final EventStoresThreadFlushAggregator threadAggregator = new EventStoresThreadFlushAggregatorImpl(
                propertyService, taskManager, eventStoreIdPool);

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final InMemoryEventStoreCount storeToFlush = new InMemoryEventStoreCount(
                getEventStoreMapKey(StatisticType.COUNT, workingInterval));

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        CellQualifier lastCellQualifierUsed = null;

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        for (int i = 1; i <= 15; i++) {
            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            lastCellQualifierUsed = cellQualifiers.get(0);

            storeToFlush.putValue(cellQualifiers.get(0), 2);

            eventTime += 60_000L;
        }

        // verify the content of the store before we add it
        assertEquals(15, storeToFlush.getSize());

        assertAllValuesInCountStore(storeToFlush, 2);

        // now pass our store to the aggregator
        threadAggregator.addFlushedStatistics(storeToFlush);

        assertEquals(1, taskManager.getCallCount());

        final List<AbstractInMemoryEventStore> storeList = taskManager.getStoresSubmitted();

        ConcurrentInMemoryEventStoreCount aggregatedStore = (ConcurrentInMemoryEventStoreCount) storeList.get(0);

        assertEquals(10, aggregatedStore.getSize());

        assertAllValuesInConcurrentCountStore(aggregatedStore, 2);

        // now force a flush of the remainder
        threadAggregator.flushAll();

        assertEquals(2, taskManager.getCallCount());

        aggregatedStore = (ConcurrentInMemoryEventStoreCount) storeList.get(1);

        assertEquals(5, aggregatedStore.getSize());

        assertAllValuesInConcurrentCountStore(aggregatedStore, 2);

    }

    /**
     * Uses a big global store size so the incoming data doesn't fill the store,
     * and a small timeout so it should flush due to timeout.
     *
     * @throws InterruptedException
     */
    @Test
    public void testAddFlushedStatisticsTimeout() throws InterruptedException {
        final long timeOutMillis = 2_000L;

        // big store, small timeout
        setProperties(1_000, timeOutMillis);

        taskManager.resetCallData();

        final InMemoryEventStoreIdPool eventStoreIdPool = new InMemoryEventStoreIdPoolImpl(propertyService);

        final EventStoresThreadFlushAggregator threadAggregator = new EventStoresThreadFlushAggregatorImpl(
                propertyService, taskManager, eventStoreIdPool);

        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final InMemoryEventStoreCount storeToFlush = new InMemoryEventStoreCount(
                getEventStoreMapKey(StatisticType.COUNT, workingInterval));

        // set up a row key builder with mocked UID service
        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        CellQualifier lastCellQualifierUsed = null;

        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        for (int i = 1; i <= 15; i++) {
            final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

            final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

            final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

            lastCellQualifierUsed = cellQualifiers.get(0);

            storeToFlush.putValue(cellQualifiers.get(0), 2);

            eventTime += 60_000L;
        }

        // verify the content of the store before we add it
        assertEquals(15, storeToFlush.getSize());

        assertAllValuesInCountStore(storeToFlush, 2);

        // now pass our store to the aggregator
        threadAggregator.addFlushedStatistics(storeToFlush);

        // store not full and not yet timed out
        assertEquals(0, taskManager.getCallCount());

        // simulate the cron scheduled method that checks for timed out stores -
        // should be none at this point.
        ((EventStoresThreadFlushAggregatorImpl) threadAggregator).flushQueue();

        assertEquals(0, taskManager.getCallCount());

        // wait for the timeout to pass
        Thread.sleep(timeOutMillis + 500L);

        // simulate the cron scheduled method that checks for timed out stores -
        // should now do a flush.
        ((EventStoresThreadFlushAggregatorImpl) threadAggregator).flushQueue();

        assertEquals(1, taskManager.getCallCount());

        final List<AbstractInMemoryEventStore> storeList = taskManager.getStoresSubmitted();

        final ConcurrentInMemoryEventStoreCount aggregatedStore = (ConcurrentInMemoryEventStoreCount) storeList.get(0);

        assertEquals(15, aggregatedStore.getSize());

        assertAllValuesInConcurrentCountStore(aggregatedStore, 2);

    }

    private EventStoreMapKey getEventStoreMapKey(final StatisticType statisticType,
            final EventStoreTimeIntervalEnum intervalEnum) {
        return new EventStoreMapKey(statisticType, 1, intervalEnum, 0, TimeUnit.MILLISECONDS);
    }

    private static class MockTaskManager<R> implements TaskManager {
        @SuppressWarnings("rawtypes")
        private final List<Task> taskList = new ArrayList<Task>();
        private final List<AbstractInMemoryEventStore> storeList = new ArrayList<>();

        public List<AbstractInMemoryEventStore> getStoresSubmitted() {
            return storeList;
        }

        public int getCallCount() {
            return taskList.size();
        }

        public void resetCallData() {
            taskList.clear();
        }


        @Override
        public <R> void execAsync(final Task<R> task) {
            throw new UnsupportedOperationException("Not used in this mock");
        }

        @Override
        public <R> void execAsync(final Task<R> task, final ThreadPool threadPool) {
            throw new UnsupportedOperationException("Not used in this mock");
        }

        @Override
        public <R> void execAsync(final Task<R> task, final TaskCallback<R> callback, final ThreadPool threadPool) {
            throw new UnsupportedOperationException("Not used in this mock");
        }

        @Override
        public <R> void execAsync(final Task<R> task, final TaskCallback<R> callback) {
            taskList.add(task);
            storeList.add(((EventStoreFlushTask) task).getMap());

            @SuppressWarnings("unchecked")
            final AbstractTaskHandler<Task<R>, R> handler = (AbstractTaskHandler<Task<R>, R>) HandlerFactory
                    .getHandler();

            final R result = handler.exec(task);

            callback.onSuccess(result);
        }

        private static class MockEventStoreFlushTaskHandler
                extends AbstractTaskHandler<EventStoreFlushTask, VoidResult> {
            @Override
            public VoidResult exec(final EventStoreFlushTask task) {
                return VoidResult.INSTANCE;
            }
        }

        private static class HandlerFactory {
            public static Object getHandler() {
                return new MockEventStoreFlushTaskHandler();
            }
        }
    }

    private void setProperties(final int maxSize, final long timeOutMillis) {
        propertyService.setProperty(HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME,
                Integer.toString(maxSize));

        for (final EventStoreTimeIntervalEnum timeInterval : EventStoreTimeIntervalEnum.values()) {
            propertyService.setProperty(HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME_PREFIX
                    + timeInterval.longName().toLowerCase(), Long.toString(timeOutMillis));
        }

        propertyService.setProperty(CommonStatisticConstants.STROOM_STATISTIC_ENGINES_PROPERTY_NAME, "hbase");

        propertyService.setProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX
                + StatisticType.COUNT.name().toLowerCase(), Integer.toString(2));

        propertyService.setProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX
                + StatisticType.VALUE.name().toLowerCase(), Integer.toString(2));

        propertyService.setProperty(
                HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_FLUSH_TASK_COUNT_LIMIT_PROPERTY_NAME_PREFIX + ".count",
                Integer.toString(5));

        propertyService.setProperty(
                HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_FLUSH_TASK_COUNT_LIMIT_PROPERTY_NAME_PREFIX + ".value",
                Integer.toString(5));

    }

    private void assertAllValuesInConcurrentCountStore(final ConcurrentInMemoryEventStoreCount countStore,
            final long expectedValue) {
        for (final Entry<RowKey, ConcurrentMap<ByteArrayWrapper, AtomicLong>> rowEntry : countStore) {
            for (final Entry<ByteArrayWrapper, AtomicLong> cellEntry : rowEntry.getValue().entrySet()) {
                assertEquals(expectedValue, cellEntry.getValue().longValue());
            }
        }
    }

    private void assertAllValuesInCountStore(final InMemoryEventStoreCount countStore, final long expectedValue) {
        for (final Entry<RowKey, Map<ByteArrayWrapper, MutableLong>> rowEntry : countStore) {
            for (final Entry<ByteArrayWrapper, MutableLong> cellEntry : rowEntry.getValue().entrySet()) {
                assertEquals(expectedValue, cellEntry.getValue().longValue());
            }
        }
    }
}
