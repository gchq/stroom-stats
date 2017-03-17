

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
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractInMemoryEventStore {
    private final EventStoreMapKey eventStoreMapKey;

    private final AtomicBoolean hasDataBeenAdded = new AtomicBoolean(false);

    /**
     * @return The number of keys in the underlying map
     */
    public abstract int getSize();

    /**
     * @param eventStoreMapKey
     *            This is the instance of a {@link EventStoreMapKey} that this
     *            store is associated with when held in an aggregator hashmap
     */
    public AbstractInMemoryEventStore(final EventStoreMapKey eventStoreMapKey) {
        this.eventStoreMapKey = eventStoreMapKey;
    }

    /**
     * @return The time interval that this event store is using
     */
    public EventStoreTimeIntervalEnum getTimeInterval() {
        return eventStoreMapKey.getTimeInterval();
    }

    /**
     * @return The {@link EventStoreMapKey} object that this store is associated
     *         with
     */
    public EventStoreMapKey getEventStoreMapKey() {
        return eventStoreMapKey;
    }

    /**
     * @return True if this is the first time data has been loaded into the
     *         store, otherwise false.
     */
    boolean isFirstDataLoadIntoStore() {
        // checks the existing value of the flag and if false sets it to true,
        // returning true if the change happened
        return hasDataBeenAdded.compareAndSet(false, true);
    }

    /**
     * Factory method to return one of the InMemoryEventStoreXXX objects
     */
    public static AbstractInMemoryEventStore getNonConcurrentInstance(final EventStoreMapKey eventStoreMapKey) {
        if (eventStoreMapKey.getStatisticType().equals(StatisticType.COUNT)) {
            return new InMemoryEventStoreCount(eventStoreMapKey);
        } else if (eventStoreMapKey.getStatisticType().equals(StatisticType.VALUE)) {
            return new InMemoryEventStoreValue(eventStoreMapKey);
        } else {
            throw new IllegalArgumentException(
                    "Not expecting the statistic type passed: " + eventStoreMapKey.getStatisticType());
        }
    }

    /**
     * Factory method to return one of the ConcurrentInMemoryEventStoreXXX
     * objects
     */
    public static AbstractInMemoryEventStore getConcurrentInstance(final EventStoreMapKey eventStoreMapKey) {
        if (eventStoreMapKey.getStatisticType().equals(StatisticType.COUNT)) {
            return new ConcurrentInMemoryEventStoreCount(eventStoreMapKey);
        } else if (eventStoreMapKey.getStatisticType().equals(StatisticType.VALUE)) {
            return new ConcurrentInMemoryEventStoreValue(eventStoreMapKey);
        } else {
            throw new IllegalArgumentException(
                    "Not expecting the statistic type passed: " + eventStoreMapKey.getStatisticType());
        }
    }

    @Override
    public String toString() {
        return "Interval: " + eventStoreMapKey.getTimeInterval() + ", Size: " + getSize();
    }
}
