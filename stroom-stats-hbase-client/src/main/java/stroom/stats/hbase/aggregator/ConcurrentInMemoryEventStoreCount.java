

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
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.util.logging.LambdaLogger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentInMemoryEventStoreCount extends AbstractInMemoryEventStore
        implements Iterable<Entry<RowKey, ConcurrentMap<ByteArrayWrapper, AtomicLong>>> {
    private final ConcurrentMap<RowKey, ConcurrentMap<ByteArrayWrapper, AtomicLong>> store = new ConcurrentHashMap<>();

    private int cellCount = 0;

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(ConcurrentInMemoryEventStoreCount.class);

    ConcurrentInMemoryEventStoreCount(final EventStoreMapKey eventStoreMapKey) {
        super(eventStoreMapKey);

        if (!eventStoreMapKey.getStatisticType().equals(StatisticType.COUNT)) {
            throw new IllegalArgumentException(
                    "The StatisticType in the passed map key does not match the inherent StatisticType of this class");
        }
    }

    /**
     * Increments the passed value into the in memory store
     *
     * @param cellQualifier The full rowkey and column qualfier containing the stat name,
     *                      partial timestamp, tag/value pairs and column qualifier
     * @param value         The value to increment the currently stored value by
     * @return True if this was the first put into the store
     */
    public boolean putValue(final CellQualifier cellQualifier, final long value) {
        LOGGER.trace("putValue called for cellQualifier: {}, value: {}", cellQualifier, value);

        return putValue(cellQualifier.getRowKey(), cellQualifier.getColumnQualifier(), value);
    }

    public boolean putValue(final RowKey rowKey, final byte[] columnQualifier, final long value) {
        LOGGER.trace(() -> String.format("putValue called for rowKey: %s, columnQualifier: %s, value: %s", rowKey,
                ByteArrayUtils.byteArrayToHex(columnQualifier), value));

        final boolean result = super.isFirstDataLoadIntoStore();

        final ByteArrayWrapper wrappedColQual = ByteArrayWrapper.of(columnQualifier);

        ConcurrentMap<ByteArrayWrapper, AtomicLong> existingCellMap = store.get(rowKey);

        // do the put if absent after the get to save us always having to build
        // the inner map just in case it is absent
        if (existingCellMap == null) {
            final ConcurrentMap<ByteArrayWrapper, AtomicLong> newCellMap = new ConcurrentHashMap<>();
            newCellMap.put(wrappedColQual, new AtomicLong(value));

            existingCellMap = store.putIfAbsent(rowKey, newCellMap);

            if (existingCellMap == null) {
                cellCount++;
            }
        }

        if (existingCellMap != null) {
            // if we are here then a value for the key existed on the get call
            // or on putIfAbsent so we can update the
            // inner map

            // make sure we have a key for our column qualifier
            final AtomicLong existingMapValue = existingCellMap.putIfAbsent(wrappedColQual, new AtomicLong(value));

            if (existingMapValue != null) {
                // the map value cannot be null as ConcurrentHashMap doesn't
                // support it so null means the key didn't
                // exist

                // now add our value to the existing one
                existingMapValue.addAndGet(value);
            } else {
                // putIfAbsent succeeded so increment the count
                cellCount++;
            }
        }

        return result;
    }

    @Override
    public int getSize() {
        return cellCount;
    }

    @Override
    public Iterator<Entry<RowKey, ConcurrentMap<ByteArrayWrapper, AtomicLong>>> iterator() {
        return store.entrySet().iterator();
    }
}
