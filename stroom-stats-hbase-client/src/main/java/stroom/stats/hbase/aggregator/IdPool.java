

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Class to act as a pool of ID values for use in event store map keys. It is
 * constructed with a pool size, so a pool size of 20 would give IDs of 0-19. A
 * request for an ID from the pool will return the last ID returned to the pool,
 * so under low load will only use a small number of different IDs. If there are
 * no IDs in the pool available for use then it will block until an ID is
 * returned.
 *
 * Uses a semaphore to control the blocking rather than just taking IDs from a
 * LinkedBlockingDeque as the latter will serialise the threads whereas the
 * semaphore will not.
 *
 * Thread safe.
 */
public class IdPool {
    private final Semaphore available;
    private final ConcurrentLinkedDeque<Integer> availablePool = new ConcurrentLinkedDeque<>();
    private final Integer[] inUseArray;
    private final Semaphore enabledPermits;
    private final AtomicBoolean isPoolEnabled;

    private final Object enableDisableLock = new Object();

    private final int poolSize;

    private static final Logger LOGGER = LoggerFactory.getLogger(IdPool.class);

    /**
     * Creates an ID pool of a given size.
     *
     * @param poolSize
     *            The number of ID values to hold in the pool.
     */
    public IdPool(final int poolSize) {
        this.poolSize = poolSize;

        if (poolSize < 1) {
            throw new IllegalArgumentException("The value for poolSize must be greater than 0");
        }

        available = new Semaphore(poolSize);
        inUseArray = new Integer[poolSize];

        isPoolEnabled = new AtomicBoolean(true);

        // start with none as we start in an enabled state so the next action on
        // this semaphore will be to drain all
        // permits
        enabledPermits = new Semaphore(0);

        // initialise the availablePool with the 'thread' ids (zero based)
        // must match the nunber of permits on the semaphore
        for (int i = 0; i < poolSize; i++) {
            availablePool.addFirst(new Integer(i));
        }
    }

    /**
     * Returns an ID from the pool. It will return the ID at the top of the
     * internal stack, so typically the ID last returned to the pool
     *
     * @return An ID from 0-n where n=poolSize-1
     * @throws InterruptedException
     */
    public int getId() throws InterruptedException {
        if (!isPoolEnabled.get()) {
            // pool is disabled so try to get a permit to continue, which should
            // block until the pool is enabled
            // we only want to try to get a permit if the pool is disabled. If
            // we tried all the time we would have to
            // handle releasing them
            // Have not used the other semaphore for this as that one has
            // permits released back which is not what we
            // want in a disabled situation.

            synchronized (this) {
                if (!isPoolEnabled.get()) {
                    // as we are in a synchronised block the first one to come
                    // in here will block on the semaphore and
                    // all other calls to getId will block on synchronised. If
                    // another thread enables the pool then the
                    // first thread will be freed by the semaphore and the
                    // following threads will then un-syncronzize as
                    // soon as they have checked the enabled state.
                    enabledPermits.acquire();

                    // TODO Think we need to immediately release the permit we
                    // have just acquired as we only got the
                    // permit so it would block if the pool was disabled. Now we
                    // have it the pool is enabled so

                }
            }
        }

        // get a permit for an ID (waiting if needed) and then pull an ID from
        // the queue and put it in the used array so
        // it can be released back later

        available.acquire();
        Integer idObject;
        idObject = availablePool.pollFirst();
        if (idObject == null) {
            throw new RuntimeException(
                    "There are no IDs left on the pool of available IDs. This implies the pool and semaphore are out of sync. This should not happen.");
        }
        final int id = idObject.intValue();
        inUseArray[id] = idObject;
        return id;
    }

    /**
     * Returns an ID to the pool for use by other threads. This method should be
     * used within a finally block to ensure the ID is returned in the event of
     * a failure.
     *
     * @param id
     *            The ID to return
     */
    public void returnId(final int id) {
        // don't have to worry about the enabled state for returning an item as
        // getId will still block if disabled.

        // We return the top one as that is the one that was most recently used.

        // requiring the ID to be passed in aloows us to ensure that the ID was
        // in use.

        Integer idObject;

        try {
            idObject = inUseArray[id];
        } catch (final ArrayIndexOutOfBoundsException e) {
            throw new RuntimeException(
                    "An ID has been returned to the pool that is outside the bounds of the pool. ID supplied: " + id
                            + ", pool size: " + poolSize,
                    e);
        }

        if (idObject == null) {
            throw new RuntimeException(
                    "Supplied ID is not found in the list of IDs in use.  May have returned the wrong ID or the pool is somehow out of sync.");
        }

        // if either of the above exceptions get thrown then it means the code
        // below does not get run so potentially an
        // ID is never returned to the pool
        // The alternative is to do the code below in a finally block but that
        // could mean we have an ID in the pool that
        // is outside of the bounds or is still in use
        // by somebody else. Either way something is fundamentally broken and a
        // code fix in calling code is required.

        // remove the object from the in use array and put it back into the pool
        inUseArray[id] = null;
        availablePool.addFirst(idObject);
        available.release();

    }

    /**
     * If not already disabled, disables the pool such that all subsequent getId
     * calls will block until enablePool is called. Calls to returnId are not
     * affected.
     */
    public void disablePool() {
        if (isPoolEnabled.get()) {
            // use a synchronized block to ensure the boolean value and permit
            // counts are in a valid state pair
            synchronized (enableDisableLock) {
                if (isPoolEnabled.compareAndSet(true, false)) {
                    // LOGGER.debug("Disabling StatisticsService ID pool");
                    // remove all available permits without blocking so no more
                    // getId calls can happen
                    enabledPermits.drainPermits();
                } else {
                    // LOGGER.debug("disablePool called but is disabled, doing
                    // nothing");
                }
            }
        } else {
            // LOGGER.debug("disablePool called but is disabled, doing
            // nothing");
        }
    }

    /**
     * Will enable the pool, if disabled, releasing any blocked getId calls
     */
    public void enablePool() {
        if (!isPoolEnabled.get()) {
            // use a synchronized block to ensure the boolean value and permit
            // counts are in a valid state pair
            synchronized (enableDisableLock) {
                if (isPoolEnabled.compareAndSet(false, true)) {
                    // LOGGER.debug("Enabling StatisticsService ID pool");
                    // enabling pool so refill with as many permits as possible
                    // we should never overflow as long as this is the ONLY
                    // place that calls release.
                    enabledPermits.release(Integer.MAX_VALUE);
                } else {
                    // LOGGER.debug("enablePool called but is enabled, doing
                    // nothing");
                }
            }
        } else {
            // LOGGER.debug("enablePool called but is enabled, doing nothing");
        }
    }

    /**
     * @return The total number of IDs in the pool, available or not.
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @return Returns true if the pool is enabled. If enabled IDs will be
     *         released when available. If disabled calls to getId will block,
     *         but calls to returnId will not.
     */
    public boolean getEnabledState() {
        return (isPoolEnabled.get());
    }

    /**
     * @return The number of permits currently available for use. Only to be
     *         used for monitoring or debugging.
     */
    public int getAvailablePermitCount() {
        return available.availablePermits();
    }
}
