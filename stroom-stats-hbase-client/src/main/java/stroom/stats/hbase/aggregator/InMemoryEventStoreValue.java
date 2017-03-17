

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Not thread safe, expected to be used in isolation
 */
public class InMemoryEventStoreValue extends AbstractInMemoryEventStore
        implements Iterable<Entry<CellQualifier, ValueCellValue>> {
    private final Map<CellQualifier, ValueCellValue> store = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryEventStoreValue.class);

    public InMemoryEventStoreValue(final EventStoreMapKey eventStoreMapKey) {
        super(eventStoreMapKey);

        if (!eventStoreMapKey.getStatisticType().equals(StatisticType.VALUE)) {
            throw new IllegalArgumentException(
                    "The StatisticType in the passed map key does not match the inherent StatisticType of this class");
        }
    }

    public boolean putValue(final CellQualifier cellQualifier, final ValueCellValue valueCellValue) {
        LOGGER.trace("putValue called for cellQualifier: {}, value: {}", cellQualifier, valueCellValue);

        final boolean result = super.isFirstDataLoadIntoStore();

        final ValueCellValue cellValue = store.get(cellQualifier);

        if (cellValue == null) {
            store.put(cellQualifier, valueCellValue);
        } else {
            // VCV is immutable so have to overwrite the existing reference with
            // the new object
            store.put(cellQualifier, cellValue.addAggregatedValues(valueCellValue));
        }
        return result;
    }

    @Override
    public int getSize() {
        return store.size();
    }

    @Override
    public Iterator<Entry<CellQualifier, ValueCellValue>> iterator() {
        return store.entrySet().iterator();
    }
}
