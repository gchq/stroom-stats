

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
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ValueCellValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentInMemoryEventStoreValue extends AbstractInMemoryEventStore
        implements Iterable<Entry<CellQualifier, AtomicReference<ValueCellValue>>> {
    private final ConcurrentMap<CellQualifier, AtomicReference<ValueCellValue>> store = new ConcurrentHashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentInMemoryEventStoreValue.class);

    ConcurrentInMemoryEventStoreValue(final EventStoreMapKey eventStoreMapKey) {
        super(eventStoreMapKey);

        if (!eventStoreMapKey.getStatisticType().equals(StatisticType.VALUE)) {
            throw new IllegalArgumentException(
                    "The StatisticType in the passed map key does not match the inherent StatisticType of this class");
        }
    }

    /**
     * Aggregates the passed value object into the map.
     *
     * @param cellQualifier
     *            The qualifier that is the key in the map
     * @param valueToAggregate
     *            The value/count to aggregate into the existing value/count (if
     *            there is one)
     * @return True if this is the first put to this store
     */
    public boolean putValue(final CellQualifier cellQualifier, final ValueCellValue valueToAggregate) {
        LOGGER.trace("putValue called for cellQualifier: {}, valueToAggregate: {}", cellQualifier, valueToAggregate);

        final boolean result = super.isFirstDataLoadIntoStore();

        // ensure we have a key/value in the map
        final AtomicReference<ValueCellValue> existingAtomicRef = store.putIfAbsent(cellQualifier,
                new AtomicReference<>(valueToAggregate));

        if (existingAtomicRef != null) {
            // Value cannot be null in ConcurrentHashMap so null here indicates
            // the key wasn't present when putIfAbsent
            // was called
            int tries = 0;

            // keep trying to aggregate in the new values in until it succeeds
            // using an atomic CheckAndSet approach
            ValueCellValue oldValue;
            do {
                oldValue = existingAtomicRef.get();

                // should never have null VCVs as we always create the atomic
                // ref with a value but just in case
                if (oldValue == null)
                    throw new RuntimeException("Found a null value in the in memory map for key: " + cellQualifier
                            + " and store time interval: " + getTimeInterval());
                tries++;
            } while (existingAtomicRef.compareAndSet(oldValue,
                    oldValue.addAggregatedValues(valueToAggregate)) == false);

            LOGGER.trace("Succeeded after {} tries", tries);
            if (tries > 100) {
                LOGGER.warn(
                        "putValue took {} tries to succeed for cellQualifier: {}, valueToAggregate: {}, mapKey: {}. Possible high contention on this cellQualifier",
                        tries, cellQualifier, valueToAggregate, super.getEventStoreMapKey());
            }

        }

        return result;
    }

    @Override
    public int getSize() {
        return store.size();
    }

    @Override
    public Iterator<Entry<CellQualifier, AtomicReference<ValueCellValue>>> iterator() {
        return store.entrySet().iterator();
    }
}
