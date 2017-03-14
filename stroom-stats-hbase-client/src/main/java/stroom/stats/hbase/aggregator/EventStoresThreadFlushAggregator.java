

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

import stroom.stats.api.StatisticType;

import java.util.Map;


public interface EventStoresThreadFlushAggregator {
    void addFlushedStatistics(AbstractInMemoryEventStore storeToFlush);

    /**
     * Flushes all in memory stores. Can be used for testing to ensure nothing
     * is left in memory
     */
    void flushAll();

    /**
     * Removes anything on the delay queue and flushes it, keeps flushing till
     * the queue is empty
     */
    void flushQueue();

    /**
     * Gets a map containing the sizes of all the in memory event stores keyed
     * by their keys
     *
     * @return A map containing the sizes of all the in memory stores at the
     *         time of calling. Due to the use of concurrent hashmaps the size
     *         value may be approximate. Will return an empty map if no stores
     *         are initialised.
     */
    Map<EventStoreMapKey, Integer> getEventStoreSizes();

    /**
     * Checks the current workload and enables/disables the appropriate ID pool
     * accordingly
     *
     * @param statisticType
     *            Controls which ID pool to work with
     */
    void enableDisableIdPool(StatisticType statisticType);
}
