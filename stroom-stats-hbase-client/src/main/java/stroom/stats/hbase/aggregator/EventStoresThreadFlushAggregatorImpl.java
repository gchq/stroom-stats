

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
import stroom.stats.api.StatisticType;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.store.task.EventStoreFlushTask;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.task.api.TaskCallbackAdaptor;
import stroom.stats.task.api.TaskManager;
import stroom.stats.task.api.VoidResult;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

@Singleton
public class EventStoresThreadFlushAggregatorImpl extends AbstractEventStoresAggregator
        implements EventStoresThreadFlushAggregator {
    // This map holds all the stats currently being aggregated up from each
    // thread specific store
    // key-value entries will look like this:
    //
    // COUNT|1|SECOND - AtomicReference<ConcurrentInMemoryEventStoreCount
    // COUNT|1|MINUTE - AtomicReference<ConcurrentInMemoryEventStoreCount
    // etc.
    // VALUE|1|SECOND - AtomicReference<ConcurrentInMemoryEventStoreValue
    // VALUE|1|MINUTE - AtomicReference<ConcurrentInMemoryEventStoreValue
    // etc.
    //
    // once each store has reach the configured maximum size the contents are
    // flushed down to persistent storage and an
    // empty store replaces it
    private final ConcurrentMap<EventStoreMapKey, AtomicReference<AbstractInMemoryEventStore>> globalMap;

    // used as a means of getting the actual instance of the map key held in the
    // global map rather than just having an
    // object that equals() one.
    private final Map<EventStoreMapKey, EventStoreMapKey> keyMap;

    // Map to hold a ReadWriteLock per map key to allow us to control access to
    // each eventStore, such that adds can
    // happen concurrently but adds cannot happen while a flush is happening.
    private final Map<EventStoreMapKey, ReentrantReadWriteLock> lockMap;

    // delay queue to hold all InMemoryEventStore objects that are created with
    // a delay so they will be flushed after a
    // time even if not full
    private final DelayQueue<EventStoreMapKey> timedOutStoresQueue;

    // In theory we could use multiple ID values to allow more concurrency
    // but for now will hard code this so there is one store per time interval
    // and type
    private static long MAP_KEY_ID = 1L;

    // map to hold a flush counter object for each statistic type to allow stst
    // type to throttled independently
    private final Map<StatisticType, AtomicInteger> flushCounterMap = new EnumMap<>(
            StatisticType.class);

    private final TaskManager taskManager;
    private final StroomPropertyService propertyService;
    private final InMemoryEventStoreIdPool idPool;

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(EventStoresThreadFlushAggregatorImpl.class);

    @Inject
    public EventStoresThreadFlushAggregatorImpl(final StroomPropertyService propertyService,
            final TaskManager taskManager,
            final InMemoryEventStoreIdPool idPool) {
        LOGGER.debug("Initialising: {}", this.getClass().getCanonicalName());

        this.taskManager = taskManager;
        this.propertyService = propertyService;
        this.idPool = idPool;

        LOGGER.debug("Initialising map and queue");

        globalMap = new ConcurrentHashMap<>();
        timedOutStoresQueue = new DelayQueue<>();

        for (final StatisticType statisticType : StatisticType.values()) {
            flushCounterMap.put(statisticType, new AtomicInteger(0));
        }

        final Map<EventStoreMapKey, EventStoreMapKey> tempKeyMap = new HashMap<>();

        final Map<EventStoreMapKey, ReentrantReadWriteLock> tempLockMap = new HashMap<>();

        // initialise the key/values in the global map as they are a known and
        // finite set
        for (final EventStoreTimeIntervalEnum timeInterval : EventStoreTimeIntervalEnum.values()) {
            final long memoryStoreTimeoutMillis = getMemoryStoreTimeoutMillis(timeInterval);

            for (final StatisticType statisticType : StatisticType.values()) {
                final EventStoreMapKey globalEventStoreKey = new EventStoreMapKey(statisticType, MAP_KEY_ID,
                        timeInterval, memoryStoreTimeoutMillis, TimeUnit.MILLISECONDS);

                final AtomicReference<AbstractInMemoryEventStore> atomicRef = new AtomicReference<>();
                // create the new event store and put it on the atomic ref
                atomicRef.set(AbstractInMemoryEventStore.getConcurrentInstance(globalEventStoreKey));

                globalMap.put(globalEventStoreKey, atomicRef);
                tempKeyMap.put(globalEventStoreKey, globalEventStoreKey);

                tempLockMap.put(globalEventStoreKey, new ReentrantReadWriteLock(true));
            }
        }

        // the keys are a finite list so this map will never change, though the
        // content of the objects in it will change
        keyMap = Collections.unmodifiableMap(tempKeyMap);
        lockMap = Collections.unmodifiableMap(tempLockMap);
    }

    @Override
    public void addFlushedStatistics(final AbstractInMemoryEventStore storeToFlush) {
        final StatisticType statisticType = storeToFlush.getEventStoreMapKey().getStatisticType();

        LOGGER.trace(() -> String.format("addFlushedStatistics called for statType: %s, store size: %s", statisticType,
                storeToFlush.getSize()));

        final EventStoreMapKey globalEventStoreKey = getMapKey(statisticType, MAP_KEY_ID,
                storeToFlush.getTimeInterval());

        LOGGER.trace("Using map key: {}", globalEventStoreKey);

        // Get the atomic ref for this key in the map
        // keys are never removed and the atomic ref itself is never changed
        // The content of the atomic ref may be null at this point but the
        // atomicRef itself will never be null
        final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef = globalMap.get(globalEventStoreKey);

        try {
            if (statisticType.equals(StatisticType.COUNT)) {
                addFlushedCountStatistics(storeToFlush, globalEventStoreKey, eventStoreAtomicRef);

            } else if (statisticType.equals(StatisticType.VALUE)) {
                addFlushedValueStatistics(storeToFlush, globalEventStoreKey, eventStoreAtomicRef);
            }

        } catch (final Exception e) {
            LOGGER.error("Error while adding flushed statistics", e);
            throw e;
        }

    }

    /**
     * Builds a {@link EventStoreMapKey} object based on the passed params and
     * then calls a get on the keyMap to return the actual instance held in the
     * inMemoryStoresMap. We need the actual object held in the map rather than
     * one that has the same hashcode because of the delay properties on the
     * {@link EventStoreMapKey}.
     *
     * @return The instance of the {@link EventStoreMapKey} held in
     *         inMemoryStoresMap matching the arguments passed.
     */
    private EventStoreMapKey getMapKey(final StatisticType statisticType, final long threadId,
            final EventStoreTimeIntervalEnum timeInterval) {
        final EventStoreMapKey eventStoreMapKey = keyMap.get(new EventStoreMapKey(statisticType, threadId, timeInterval,
                getMemoryStoreTimeoutMillis(timeInterval), TimeUnit.MILLISECONDS));

        return eventStoreMapKey;
    }

    private void addKeyToDelayQueue(final EventStoreMapKey eventStoreMapKey) {
        // reset the delay on the map key instance
        eventStoreMapKey.resetDelay(getMemoryStoreTimeoutMillis(eventStoreMapKey.getTimeInterval()),
                TimeUnit.MILLISECONDS);

        // take the key off the delay queue to save unnecessary processing later
        timedOutStoresQueue.remove(eventStoreMapKey);

        // put the key to the store on the delay queue so if it doesn't get
        // flushed for being full it will get
        // flushed within the desired time period
        timedOutStoresQueue.add(eventStoreMapKey);
    }

    @Override
    public Map<EventStoreMapKey, Integer> getEventStoreSizes() {
        final Map<EventStoreMapKey, Integer> sizeMap = new HashMap<>();

        if (globalMap != null) {
            for (final Entry<EventStoreMapKey, AtomicReference<AbstractInMemoryEventStore>> entry : globalMap
                    .entrySet()) {
                sizeMap.put(entry.getKey().copy(), entry.getValue().get().getSize());
            }
        }
        return sizeMap;
    }

    /**
     * @param storeToAdd
     *            The store being added into the global store
     * @param globalEventStoreKey
     *            The map key for the global store map
     * @param eventStoreAtomicRef
     *            The atomic reference of the store in the global map
     */
    private void addFlushedCountStatistics(final AbstractInMemoryEventStore storeToAdd,
            final EventStoreMapKey globalEventStoreKey,
            final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef) {
        LOGGER.trace(() -> String.format("addFlushedCountStatistics called for key: %s, store size: %s",
                globalEventStoreKey, storeToAdd.getSize()));

        // get the store to aggregate into. It will never be null as they are
        // pre-created in the constructor and a new
        // one is created on each flush
        ConcurrentInMemoryEventStoreCount globalStore = (ConcurrentInMemoryEventStoreCount) eventStoreAtomicRef.get();

        // loop through all the entries in the flushed map and put them into the
        // global map
        for (final Entry<RowKey, Map<ByteArrayWrapper, MutableLong>> rowEntry : (InMemoryEventStoreCount) storeToAdd) {
            if (rowEntry.getValue() != null) {
                for (final Entry<ByteArrayWrapper, MutableLong> cellEntry : rowEntry.getValue().entrySet()) {
                    // get a 'read' lock for this map key to do the add. Flush
                    // operations will get a write lock so will
                    // block all adds for the duration of the flush
                    final ReadLock readLock = lockMap.get(globalEventStoreKey).readLock();
                    readLock.lock();

                    try {
                        // put the flushed map entry into the global map
                        if (globalStore.putValue(rowEntry.getKey(), cellEntry.getKey().getBytes(),
                                cellEntry.getValue().longValue()) == true) {
                            // first put to this store so put it on the delay
                            // queue
                            addKeyToDelayQueue(globalEventStoreKey);
                        }

                    } finally {
                        readLock.unlock();
                    }

                    if (flushIfNeeded(globalEventStoreKey, eventStoreAtomicRef)) {
                        // flush happened so get the new store reference
                        globalStore = (ConcurrentInMemoryEventStoreCount) eventStoreAtomicRef.get();
                    }
                }
            }
        }
    }

    /**
     * Not thread safe so must operate within a synchronised block
     *
     * @param storeToAdd
     *            The store being added into the global store
     * @param globalEventStoreKey
     *            The map key for the global store map
     * @param eventStoreAtomicRef
     *            The atomic reference of the store in the global map
     */
    private void addFlushedValueStatistics(final AbstractInMemoryEventStore storeToAdd,
            final EventStoreMapKey globalEventStoreKey,
            final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef) {
        LOGGER.trace(() -> String.format("addFlushedValueStatistics called for key: %s, store size: %s",
                globalEventStoreKey, storeToAdd.getSize()));

        // get the store to aggregate into. It will never be null as they are
        // pre-created in the constructor and a new
        // one is created on each flush
        ConcurrentInMemoryEventStoreValue globalStore = (ConcurrentInMemoryEventStoreValue) eventStoreAtomicRef.get();

        // loop through all the entries in the flushed map and put them into the
        // global map
        for (final Entry<CellQualifier, ValueCellValue> entry : (InMemoryEventStoreValue) storeToAdd) {
            // get a 'read' lock for this map key to do the add. Flush
            // operations will get a write lock so will
            // block all adds for the duration of the flush
            final ReadLock readLock = lockMap.get(globalEventStoreKey).readLock();
            readLock.lock();

            try {
                // put the flushed map entry into the global map
                if (globalStore.putValue(entry.getKey(), entry.getValue()) == true) {
                    // first put to this store so put it on the delay queue
                    addKeyToDelayQueue(globalEventStoreKey);
                }

            } finally {
                readLock.unlock();
            }

            if (flushIfNeeded(globalEventStoreKey, eventStoreAtomicRef)) {
                // flush happened so get the new store reference
                globalStore = (ConcurrentInMemoryEventStoreValue) eventStoreAtomicRef.get();
            }
        }
    }

    /**
     * Determines if a flush is needed on the event store at the passed atomic
     * ref. Determination based on size of store and time since it was created.
     * If it is needed it flushes the store and replaces the store with a new
     * instance
     *
     * @param eventStoreAtomicRef
     *            The ref to the store
     * @return True if a flush happens, false if not
     */
    private boolean flushIfNeeded(final EventStoreMapKey globalEventStoreKey,
            final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef) {
        // check if the store is full up and if so flush it to the master store
        if (eventStoreAtomicRef.get() != null && eventStoreAtomicRef.get().getSize() >= getMemoryStoreMaxSize()) {
            return flushThreadStore(globalEventStoreKey, eventStoreAtomicRef, false);
        } else {
            return false;
        }
    }

    private boolean flushThreadStore(final EventStoreMapKey globalEventStoreKey,
            final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef, final boolean isForcedFlush) {
        boolean result = false;
        incrementFlushesInProgress(globalEventStoreKey.getStatisticType());

        try {
            enableDisableIdPool(globalEventStoreKey.getStatisticType());

            // swap out the store to flush with a new one
            // if the swap doesn't happen (because another thread beat us to it)
            // then do no more

            final AbstractInMemoryEventStore storeToFlush = swapInMemoryEventStore(globalEventStoreKey,
                    eventStoreAtomicRef, isForcedFlush);

            if (storeToFlush != null) {
                LOGGER.debug(() -> String.format("flushThreadStore called for store: %s, type: %s, store size: %s",
                        storeToFlush.getTimeInterval(), globalEventStoreKey.getStatisticType(),
                        storeToFlush.getSize()));

                // create a task for this flush operation and hand it to the
                // task manager to process asynchronously
                taskManager.execAsync(new EventStoreFlushTask(storeToFlush, isForcedFlush),
                        new TaskCallbackAdaptor<VoidResult>() {
                            @Override
                            public void onSuccess(final VoidResult result) {
                                decrementFlushesInProgress(globalEventStoreKey.getStatisticType());
                                enableDisableIdPool(globalEventStoreKey.getStatisticType());
                                LOGGER.debug(() -> String.format("EventStoreFlushTask completed successfully for store: %s",
                                        storeToFlush.getTimeInterval()));

                            }

                            @Override
                            public void onFailure(final Throwable t) {
                                decrementFlushesInProgress(globalEventStoreKey.getStatisticType());
                                enableDisableIdPool(globalEventStoreKey.getStatisticType());
                                LOGGER.error("EventStoreFlushTask failed for store: {}", storeToFlush.getTimeInterval(), t);
                            }
                        });
                result = true;

            } else {
                LOGGER.trace("Swap didn't happen, so no flush required");
            }
        } catch (final Throwable t) {
            decrementFlushesInProgress(globalEventStoreKey.getStatisticType());
            enableDisableIdPool(globalEventStoreKey.getStatisticType());
            throw t;
        }

        return result;
    }

    @Override
    public void enableDisableIdPool(final StatisticType statisticType) {
        final int maxFlushTasks = getFlushTaskLimitCount(statisticType);

        LOGGER.debug("enableDisableIdPool called with flushesInProgressCounter [{}].  maxFlushTasks: [{}]",
                getFlushesInProgress(statisticType), maxFlushTasks);

        synchronized (this) {
            if (getFlushesInProgress(statisticType) >= maxFlushTasks) {
                // too many memory stores backed up so halt the puts at the
                // front end
                idPool.disablePool(statisticType);
            } else {
                // below our limit so enable the id pool if it is disabled
                idPool.enablePool(statisticType);
            }
        }
    }

    /**
     * Not thread safe so must be done in a synchronised block.
     *
     */
    private AbstractInMemoryEventStore getOrCreateEventStore(final EventStoreMapKey globalEventStoreKey,
            final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef) {
        LOGGER.trace(() -> String.format("getOrCreateEventStore called for interval: %s, mapKey: %s", globalEventStoreKey.getTimeInterval(),
                globalEventStoreKey));

        // get the store for this timeInterval, if it doesn't exist create it
        AbstractInMemoryEventStore store = eventStoreAtomicRef.get();

        if (eventStoreAtomicRef.compareAndSet(null,
                AbstractInMemoryEventStore.getConcurrentInstance(globalEventStoreKey))) {
            // it was null and we have now created a store so grab the new store
            store = eventStoreAtomicRef.get();

            LOGGER.trace("Reference was null so creating a new store");
        } else {
            LOGGER.trace("Reference was not null so using existing");
        }

        return store;
    }

    /**
     * Attempts to set the atomic reference to another variable and then null
     * the atomic reference so puts can start on an empty store while the
     * swapped out store is flushed
     *
     * @param globalEventStoreKey
     *            The Key to map to find the store to swap out
     * @return The store to be flushed. Null if the swap hasn't happened and
     *         there is nothing to flush. This will likely be caused by the
     *         store having filled up and flushed before this time out flush.
     */
    private AbstractInMemoryEventStore swapInMemoryEventStore(final EventStoreMapKey globalEventStoreKey,
            final AtomicReference<AbstractInMemoryEventStore> eventStoreAtomicRef, final boolean isForcedFlush) {
        LOGGER.trace("getOrCreateEventStore called for eventStoreAtomicRef: {}", eventStoreAtomicRef);

        // there could be multiple threads working on the event store for this
        // map key:
        // worker thread - doing the puts and flushes if full, spawned by the
        // stream processing
        // task thread - doing flushes for event stores that have timed out
        // if a task thread tries to flush while a worker thread is putting then
        // the map value may be null if the flush
        // has happened
        // so it should create a new InMemoryEventStore object and put into that

        // AtomicReference<AbstractInMemoryEventStore> atomicRef =
        // globalMap.get(globalEventStoreKey);
        AbstractInMemoryEventStore storeToFlush = null;

        // get a 'write' lock for this map key to stop all add operations
        // against this key while the flush happens.
        // Ensures nobody touches the underlying eventStore
        final WriteLock writeLock = lockMap.get(globalEventStoreKey).writeLock();
        writeLock.lock();

        try {
            storeToFlush = eventStoreAtomicRef.get();

            final long delay = (globalEventStoreKey == null ? -1 : globalEventStoreKey.getDelay(TimeUnit.MILLISECONDS));
            final int size = (storeToFlush == null ? -1 : storeToFlush.getSize());

            // TODO If this has been initiated by an item on the delay queue
            // being ready then we are comparing two
            // different timouts, the one on the map key and the one on the
            // store. If they don't tie up then we may
            // ignore the store. Ideally we need to get the instance of the
            // actual map key but the only way to do
            // that
            // is to iterate over the map keys which we don't want to do.

            // ensure we have a store object to swap out that it is still in a
            // state worthy of a swap
            if (storeToFlush != null && size > 0 && (isForcedFlush || delay <= 0 || size >= getMemoryStoreMaxSize())) {
                // Set the content of the atomicReference to a new EventStore
                // instance
                // Equivalent to volatile so change should be immediately
                // visible to other threads

                eventStoreAtomicRef.set(null);
                getOrCreateEventStore(globalEventStoreKey, eventStoreAtomicRef);

                LOGGER.trace("removing map entry for eventStoreAtomicRef: {}", eventStoreAtomicRef);
            } else {
                // not swapping so return null
                storeToFlush = null;
                LOGGER.trace("Nothing to flush, storeToFlush: {}, delay: {}, size: {}", storeToFlush, delay, size);
            }
        } finally {
            writeLock.unlock();
        }

        return storeToFlush;
    }

    @Override
    public void flushAll() {
        LOGGER.debug("flushAll called");

        for (final EventStoreMapKey eventStoreMapKey : globalMap.keySet()) {
            // LOGGER.debug("flushThreadStore called for store with interval: %s
            // and type: %s",
            // eventStoreMapKey.getTimeInterval(),
            // eventStoreMapKey.getStatisticType());
            flushThreadStore(eventStoreMapKey, globalMap.get(eventStoreMapKey), true);
        }
    }

    @Override
    public void flushQueue() {
        EventStoreMapKey eventStoreMapKey;

        // flush all keys visible on the queue now
        while ((eventStoreMapKey = timedOutStoresQueue.poll()) != null) {
            flushThreadStore(eventStoreMapKey, globalMap.get(eventStoreMapKey), false);
        }
    }

    private int getMemoryStoreMaxSize() {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME);
    }

    private long getMemoryStoreTimeoutMillis(final EventStoreTimeIntervalEnum timeInterval) {
        return propertyService.getLongPropertyOrThrow(HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME_PREFIX
                        + timeInterval.longName().toLowerCase());
    }

    private int getFlushTaskLimitCount(final StatisticType statisticType) {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX
                        + statisticType.name().toLowerCase());
    }

    private void incrementFlushesInProgress(final StatisticType statisticType) {
        flushCounterMap.get(statisticType).incrementAndGet();
    }

    private void decrementFlushesInProgress(final StatisticType statisticType) {
        flushCounterMap.get(statisticType).decrementAndGet();
    }

    private int getFlushesInProgress(final StatisticType statisticType) {
        return flushCounterMap.get(statisticType).get();
    }
}
