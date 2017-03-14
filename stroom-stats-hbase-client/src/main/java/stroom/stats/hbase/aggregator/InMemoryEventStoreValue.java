

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
