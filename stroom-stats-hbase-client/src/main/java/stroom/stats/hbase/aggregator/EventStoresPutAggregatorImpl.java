

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
import stroom.stats.api.StatisticType;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.structure.AddEventOperation;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ColumnQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class EventStoresPutAggregatorImpl extends AbstractEventStoresAggregator implements EventStoresPutAggregator {
    // map to hold the in memory stores, keyed on the stat type (COUNT|VALUE),
    // thread ID and then on the event store
    // time interval
    // the idea is each thread gets InMemoryEventStore objects from the map
    // using its thread ID, so each one will be
    // only used by that thread
    // providing thread isolation. The only caveat to this is the background
    // task that handles timed out objects that
    // need to be flushed before
    // they are full.

    // Once a the InMemoryEventStore is full up (or has been in existence beyond
    // a defined time out period) it is
    // replaced by a new empty InMemoryStore. The data in the used one is loaded
    // into
    // the next most coarse event store to aggregate up the data to the next
    // time interval. The used store is then put
    // on a queue
    // to be merged into the master (i.e. non thread specific)
    // InMemoryEventStore.

    private final ConcurrentMap<EventStoreMapKey, AtomicReference<AbstractInMemoryEventStore>> inMemoryStoresMap;

    // used as a means of getting the actual instance of the map key held in the
    // global map rather than just having an
    // object that equals() one.
    private final Map<EventStoreMapKey, EventStoreMapKey> keyMap;

    // delay queue to hold all InMemoryEventStore objects that are created with
    // a delay so they will be flushed after a
    // time even if not full
    private final DelayQueue<EventStoreMapKey> timedOutStoresQueue;

    private final EventStoresThreadFlushAggregator eventStoresThreadFlushAggregator;

    private final StroomPropertyService propertyService;

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(EventStoresPutAggregatorImpl.class);

    private ScheduledExecutorService debugSnapshotScheduler;

    private final InMemoryEventStoreIdPool idPool;

    private ThreadLocal<AtomicInteger> debugRecursionLevelCounter;

    @Inject
    public EventStoresPutAggregatorImpl(final EventStoresThreadFlushAggregator eventStoresThreadFlushAggregator,
            final StroomPropertyService propertyService,
            final InMemoryEventStoreIdPool idPool) {
        LOGGER.debug("Initialising: {}", this.getClass().getCanonicalName());

        this.eventStoresThreadFlushAggregator = eventStoresThreadFlushAggregator;
        this.propertyService = propertyService;
        this.idPool = idPool;

        LOGGER.debug("Initialising map and queue");

        inMemoryStoresMap = new ConcurrentHashMap<>();

        timedOutStoresQueue = new DelayQueue<>();

        Map<EventStoreMapKey, EventStoreMapKey> tempKeyMap = new HashMap<>();

        // initialise the key/values in the global map as they are a known and
        // finite set
        for (final EventStoreTimeIntervalEnum timeInterval : EventStoreTimeIntervalEnum.values()) {
            final long memoryStoreTimeoutMillis = getMemoryStoreTimeoutMillis(timeInterval);

            for (final StatisticType statisticType : StatisticType.values()) {
                final int idPoolSize = idPool.getPoolSize(statisticType);

                for (int i = 0; i < idPoolSize; i++) {
                    final EventStoreMapKey globalEventStoreKey = new EventStoreMapKey(statisticType, i, timeInterval,
                            memoryStoreTimeoutMillis, TimeUnit.MILLISECONDS);

                    final AtomicReference<AbstractInMemoryEventStore> atomicRef = new AtomicReference<>();
                    // create the new event store and put it on the atomic ref
                    atomicRef.set(AbstractInMemoryEventStore.getNonConcurrentInstance(globalEventStoreKey));

                    inMemoryStoresMap.put(globalEventStoreKey, atomicRef);
                    tempKeyMap.put(globalEventStoreKey, globalEventStoreKey);
                }
            }
        }

        // the keys are a finite list so this map will never change, though the
        // reference that the atomicRefs point to
        // will change
        keyMap = Collections.unmodifiableMap(tempKeyMap);
        tempKeyMap = null;

        // thread local variable to keep a track of what level of recursion we
        // are at for store flushes as one flush may
        // result in up to 3 others
        if (LOGGER.isDebugEnabled()) {
            debugRecursionLevelCounter = new ThreadLocal<AtomicInteger>() {
                @Override
                protected AtomicInteger initialValue() {
                    return new AtomicInteger(0);
                }
            };
        }
    }

    @Override
    public void putEvents(final List<AddEventOperation> addEventOperations, final StatisticType statisticType) {
        if (LOGGER.isDebugEnabled()) {
            debugRecursionLevelCounter.set(new AtomicInteger(0));
        }

        LOGGER.trace(
                () -> String.format("putEvents called for addEventOperations count: %s", addEventOperations.size()));

        int threadId = -1;

        try {
            try {
                // get an ID from the pool to use as part of the map key
                // this gives us the ability to control the number of stores in
                // use at a time.
                // Holding this id gives us exclusive access to all stores with
                // that id/stattype in their map key, i.e.
                // 4 stores (1 per time interval)
                threadId = idPool.getId(statisticType);

                LOGGER.trace("Using ID: {}", threadId);

                if (statisticType.equals(StatisticType.COUNT)) {
                    for (final AddEventOperation addEventOperation : addEventOperations) {
                        putCountTypeValue(threadId, addEventOperation.getTimeInterval(),
                                addEventOperation.getCellQualifier(),
                                addEventOperation.getRolledUpStatisticEvent().getCount());
                    }

                } else if (statisticType.equals(StatisticType.VALUE)) {
                    for (final AddEventOperation addEventOperation : addEventOperations) {
                        final ValueCellValue valueCellValue = new ValueCellValue(
                                addEventOperation.getRolledUpStatisticEvent().getValue());

                        putValueTypeValue(threadId, addEventOperation.getTimeInterval(),
                                addEventOperation.getCellQualifier(), valueCellValue);
                    }
                }
            } finally {
                // return the ID to the pool for another thread to use
                idPool.returnId(statisticType, threadId);
            }
        } catch (final InterruptedException e) {
            LOGGER.error("Thread interrupted unexpectedly, {} statistic events not put into the store.",
                    addEventOperations.size(), e);
        }
    }

    private void putCountTypeValue(final int threadId, final EventStoreTimeIntervalEnum timeInterval,
            final CellQualifier cellQualifier, final long value) {
        // get the map key object held in the inMemory store
        final EventStoreMapKey eventStoreMapKey = getMapKey(StatisticType.COUNT, threadId, timeInterval);

        // Ensure we have a key and an initialised atomic ref
        final AtomicReference<AbstractInMemoryEventStore> atomicRef = inMemoryStoresMap.get(eventStoreMapKey);

        // Synchronise on the atomic ref as the only threads interested in this
        // object are the one thread (enforced by
        // the id pool) doing the puts
        // and the possible many threads occasionally trying to do a timeout
        // flush, thus contention should be minimal
        synchronized (atomicRef) {
            // InMemoryEventStoreCount store = (InMemoryEventStoreCount)
            // getOrCreateEventStore(timeInterval,
            // eventStoreMapKey, atomicRef);
            final InMemoryEventStoreCount store = (InMemoryEventStoreCount) atomicRef.get();

            // put the event into the store
            if (store.putValue(cellQualifier, value) == true) {
                // first put to this store so reset the delay and queue it
                addKeyToDelayQueue(eventStoreMapKey);
            }

            flushIfNeeded(threadId, eventStoreMapKey, store.getSize(), atomicRef);
        }
    }

    private void putValueTypeValue(final int threadId, final EventStoreTimeIntervalEnum timeInterval,
            final CellQualifier cellQualifier, final ValueCellValue valueCellValue) {
        // get the map key object held in the inMemory store
        final EventStoreMapKey eventStoreMapKey = getMapKey(StatisticType.VALUE, threadId, timeInterval);

        // Ensure we have a key and an initialised atomic ref
        final AtomicReference<AbstractInMemoryEventStore> atomicRef = inMemoryStoresMap.get(eventStoreMapKey);

        // Synchronise on the atomic ref as the only threads interested in this
        // object are the one thread (enforced by
        // the id pool) doing the puts
        // and the possible many threads occasionally trying to do a timeout
        // flush, thus contention should be minimal
        synchronized (atomicRef) {
            // InMemoryEventStoreValue store = (InMemoryEventStoreValue)
            // getOrCreateEventStore(timeInterval,
            // eventStoreMapKey, atomicRef);
            final InMemoryEventStoreValue store = (InMemoryEventStoreValue) atomicRef.get();

            // put the event into the store
            if (store.putValue(cellQualifier, valueCellValue) == true) {
                // first put to this store so reset the delay and queue it
                addKeyToDelayQueue(eventStoreMapKey);
            }

            flushIfNeeded(threadId, eventStoreMapKey, store.getSize(), atomicRef);
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

        // have to remove and re-add the map key to the delay queue as it cannot
        // re-order items

        // take the key off the delay queue to save unnecessary processing later
        timedOutStoresQueue.remove(eventStoreMapKey);

        // put the key to the store on the delay queue so if it doesn't get
        // flushed for being full it will get
        // flushed within the desired time period
        timedOutStoresQueue.add(eventStoreMapKey);
    }

    private void flushIfNeeded(final int threadId, final EventStoreMapKey eventStoreMapKey, final int storeSize,
            final AtomicReference<AbstractInMemoryEventStore> atomicRef) {
        LOGGER.trace("flushIfNeeded called for store with size: {}", storeSize);

        // check if the store is full up and if so flush it to the master store
        if (storeSize >= getMemoryStoreMaxSize()) {
            LOGGER.trace("Flushing");
            flushThreadStore(threadId, eventStoreMapKey, atomicRef, false);
        } else {
            LOGGER.trace("Not flushing");
        }
    }

    private AbstractInMemoryEventStore getOrCreateEventStore(final EventStoreMapKey eventStoreMapKey,
            final AtomicReference<AbstractInMemoryEventStore> atomicRef) {
        LOGGER.trace(() -> String.format("getOrCreateEventStore called for interval: %s, mapKey: %s", eventStoreMapKey.getTimeInterval(),
                eventStoreMapKey));

        // get the store for this timeInterval, if it doesn't exist create it
        AbstractInMemoryEventStore store = atomicRef.get();

        if (atomicRef.compareAndSet(null, AbstractInMemoryEventStore.getNonConcurrentInstance(eventStoreMapKey))) {
            // it was null and we have now created a store so grab the new store
            // and put it on the delay queue
            store = atomicRef.get();

            // // reset the delay on the map key instance
            // eventStoreMapKey.resetDelay(getMemoryStoreTimeoutMillis(),
            // TimeUnit.MILLISECONDS);
            //
            // // take the key off the delay queue to save unnecessary
            // processing later
            // timedOutStoresQueue.remove(eventStoreMapKey);
            //
            // // put the key to the store on the delay queue so if it doesn't
            // get flushed for being full it will get
            // // flushed within the desired time period
            // timedOutStoresQueue.add(eventStoreMapKey);

            LOGGER.trace("Creating a new store");
        } else {
            LOGGER.trace("Using existing store");
        }

        return store;
    }

    private void flushThreadStore(final int threadId, final EventStoreMapKey eventStoreMapKey,
            final AtomicReference<AbstractInMemoryEventStore> atomicRef, final boolean isForcedFlush) {
        // It is possible for threadId to be NOT equal to the id value inside
        // eventStoreMapKey. This is the case for
        // flush operations called from outside. They will grab any id and this
        // will be used as the id for any store
        // roll-ups. The synchronised block provides the protection against
        // multiple operations trying to touch the same
        // event store object.

        // Need to synchronise on the map value in case the delayQueue
        // processing is also trying to do a flush at the
        // same time

        // there could be multiple threads working on the event store for this
        // map key:
        // worker thread - doing the puts and flushes if full, spawned by the
        // stream processing
        // task thread - doing flushes for event stores that have timed out
        // if a task thread tries to flush while a worker thread is putting then
        // the map value may be null if the flush
        // has happened
        // so it should create a new InMemoryEventStore object and put into that

        // synchronise on the atomic ref as this will ensure the one thread
        // doing puts to this atomic ref are not in the
        // middle of puts during a flush

        synchronized (atomicRef) {
            // swap out the store to flush with a new one
            // if the swap doesn't happen (because another thread beat us to it)
            // then do no more

            final AbstractInMemoryEventStore storeToFlush = swapInMemoryEventStore(eventStoreMapKey, atomicRef,
                    isForcedFlush);

            if (storeToFlush != null) {
                    LOGGER.debug(() -> String.format( "flushThreadStore called for ThreadId: %s, Store: %s, type: %s, store size: %s, recursion level: %s",
                            threadId, eventStoreMapKey.getTimeInterval(), eventStoreMapKey.getStatisticType(),
                            storeToFlush == null ? "" : storeToFlush.getSize(), debugRecursionLevelCounter.get().get()));

                    debugRecursionLevelCounter.get().incrementAndGet();

                // storeToFlush is no longer being used by the put methods so we
                // can work on it in isolation

                // synchronously flush the data from this thread's event store
                // up into the global store
                eventStoresThreadFlushAggregator.addFlushedStatistics(storeToFlush);

                // establish if there is an event store with a coarser
                // granularity than this one
                // and if so roll up this data into that store
                final Optional<EventStoreTimeIntervalEnum> nextInterval = EventStoreTimeIntervalHelper
                        .getNextBiggest(storeToFlush.getTimeInterval());

                if (nextInterval.isPresent()) {
                    if (eventStoreMapKey.getStatisticType().equals(StatisticType.COUNT)) {
                        rollUpCountStore(threadId, (InMemoryEventStoreCount) storeToFlush, nextInterval.get());
                    } else if (eventStoreMapKey.getStatisticType().equals(StatisticType.VALUE)) {
                        rollUpValueStore(threadId, (InMemoryEventStoreValue) storeToFlush, nextInterval.get());
                    }
                }
            }
            // after this point nothing should hold a reference to the flushed
            // store object so it should be garbage
            // collected
        }
    }

    private void rollUpCountStore(final int threadId, final InMemoryEventStoreCount storeToRollUp,
            final EventStoreTimeIntervalEnum nextInterval) {
        LOGGER.debug(() -> String.format("rollUpCountStore called for store with size: %s and new interval: %s", storeToRollUp.getSize(),
                nextInterval));

        for (final Entry<RowKey, Map<ColumnQualifier, MutableLong>> rowEntry : storeToRollUp) {
            for (final Entry<ColumnQualifier, MutableLong> cellEntry : rowEntry.getValue().entrySet()) {
                // need to convert the rowkey and colQual to the new time
                // interval before calling putCountValue
                final CellQualifier newCellQualifier = SimpleRowKeyBuilder.convertCellQualifier(
                        rowEntry.getKey(),
                        cellEntry.getKey(),
                        storeToRollUp.getTimeInterval(),
                        nextInterval);

                // recursively call putCountValue to roll the current value up
                // into the next coarser event store
                putCountTypeValue(threadId, nextInterval, newCellQualifier, cellEntry.getValue().longValue());
            }
        }
    }

    private void rollUpValueStore(final int threadId, final InMemoryEventStoreValue storeToRollUp,
            final EventStoreTimeIntervalEnum nextInterval) {
        LOGGER.trace(() -> String.format("rollUpCountStore called for store with size: %s and new interval: %s",
                storeToRollUp.getSize(), nextInterval));

        // iterate over all the stats from the store to flush and then for each
        // stat build a new cell qualifier
        // appropriate to the next coarser store.
        for (final Entry<CellQualifier, ValueCellValue> entry : storeToRollUp) {
            // need to convert the rowkey and colQual to the new time interval
            // before calling putCountValue
            final CellQualifier newCellQualifier = SimpleRowKeyBuilder.convertCellQualifier(entry.getKey(),
                    nextInterval);

            // recursively call putCountValue to roll the current value up into
            // the next coarser event store
            putValueTypeValue(threadId, nextInterval, newCellQualifier, entry.getValue());
        }
    }

    /**
     * Attempts to set the atomic reference to another variable and then null
     * the atomic reference so puts can start on an empty store while the
     * swapped out store is flushed. Must be done under a synchronized block on
     * atomicRef
     *
     * @param eventStoreMapKey
     *            The Key to map to find the store to swap out
     * @return The store to be flushed. Null if the swap hasn't happened and
     *         there is nothing to flush. This will likely be caused by the
     *         store having filled up and flushed before this time out flush.
     */
    private AbstractInMemoryEventStore swapInMemoryEventStore(final EventStoreMapKey eventStoreMapKey,
            final AtomicReference<AbstractInMemoryEventStore> atomicRef, final boolean isForcedFlush) {
        LOGGER.trace("swapInMemoryEventStore called for mapKey: {}", eventStoreMapKey);

        AbstractInMemoryEventStore storeToFlush = atomicRef.get();

        final long delay = eventStoreMapKey.getDelay(TimeUnit.MILLISECONDS);
        final int size = storeToFlush.getSize();

        // ensure we have a store object to swap out that it is still in a state
        // worthy of a swap
        if (storeToFlush != null && storeToFlush.getSize() > 0
                && (isForcedFlush || delay <= 0 || size >= getMemoryStoreMaxSize())) {
            // remove the reference the hashmap is holding to our store being
            // flushed. storeToFlush now holds the only
            // reference.
            atomicRef.set(null);
            getOrCreateEventStore(eventStoreMapKey, atomicRef);

            LOGGER.trace("removing map entry for mapKey: {}", eventStoreMapKey);
        } else {
            // not swapping so return null
            storeToFlush = null;
            LOGGER.trace("Nothing to flush, delay: {}, size: {}", delay, size);
        }

        return storeToFlush;
    }

    @Override
    public void flushAll() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(() -> String.format("flushAll called, map keys in store (size: %s):", inMemoryStoresMap.size()));
            // for (EventStoreMapKey eventStoreMapKey :
            // inMemoryStoresMap.keySet()) {
            // LOGGER.debug(" %s", eventStoreMapKey);
            // }
        }

        // loop through all the stores in the map in order of increasing
        // interval size.
        // this is because of the recursive flush code that rolls up data into
        // the next coarser store
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            for (final EventStoreMapKey eventStoreMapKey : inMemoryStoresMap.keySet()) {
                if (eventStoreMapKey.getTimeInterval().equals(interval)) {
                    int threadId = -1;
                    try {
                        try {
                            // this ID will be used to rollup the store being
                            // flushed into a coarser one, unless it is
                            // the most coarse store
                            threadId = idPool.getId(eventStoreMapKey.getStatisticType());
                            flushThreadStore(threadId, eventStoreMapKey, inMemoryStoresMap.get(eventStoreMapKey), true);
                        } finally {
                            idPool.returnId(eventStoreMapKey.getStatisticType(), threadId);
                        }
                    } catch (final InterruptedException e) {
                        LOGGER.error("Thread interrupted unexpectedly flushing store with key: {}", eventStoreMapKey,
                                e);
                    }
                }
            }
        }

        // now flush the down stream stores
        eventStoresThreadFlushAggregator.flushAll();
    }

    @Override
    public void flushQueue() {
        EventStoreMapKey eventStoreMapKey;

        // flush all keys visible on the queue now
        while ((eventStoreMapKey = timedOutStoresQueue.poll()) != null) {
            int threadId = -1;
            try {
                try {
                    // this ID will be used to rollup the store being flushed
                    // into a coarser one, unless it is the most
                    // coarse store
                    threadId = idPool.getId(eventStoreMapKey.getStatisticType());
                    flushThreadStore(threadId, eventStoreMapKey, inMemoryStoresMap.get(eventStoreMapKey), false);
                } finally {
                    idPool.returnId(eventStoreMapKey.getStatisticType(), threadId);
                }
            } catch (final InterruptedException e) {
                LOGGER.error("Thread interrupted unexpectedly flushing store with key: {}", eventStoreMapKey, e);
            }
        }
    }

    @Override
    public Map<EventStoreMapKey, Integer> getEventStoreSizes() {
        final Map<EventStoreMapKey, Integer> sizeMap = new HashMap<>();

        if (inMemoryStoresMap != null) {
            for (final Entry<EventStoreMapKey, AtomicReference<AbstractInMemoryEventStore>> entry : inMemoryStoresMap
                    .entrySet()) {
                sizeMap.put(entry.getKey().copy(), entry.getValue().get().getSize());
            }
        }
        return sizeMap;
    }

    private int getMemoryStoreMaxSize() {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME);
    }

    private long getMemoryStoreTimeoutMillis(final EventStoreTimeIntervalEnum timeInterval) {
        return propertyService
                .getLongPropertyOrThrow(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME_PREFIX
                        + timeInterval.longName().toLowerCase());
    }
}
