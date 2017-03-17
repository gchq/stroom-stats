

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

import stroom.stats.api.StatisticType;
import stroom.stats.hbase.structure.AddEventOperation;

import java.util.List;
import java.util.Map;

public interface EventStoresPutAggregator {
    /**
     * Method to put a multiple events of the same interval and type into the in
     * memory store. This should only be called for the finest time interval. It
     * will cause the event to be aggregated up into the coarser event stores
     *
     * @param addEventOperations
     *            List of event operations to add
     * @param statisticType
     *            The type of all events being passed, COUNT or VALUE.
     */
    void putEvents(List<AddEventOperation> addEventOperations, StatisticType statisticType);

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
}
