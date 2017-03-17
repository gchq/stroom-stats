

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
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.util.logging.LambdaLogger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * NOT thread safe, expected to be used by a single thread in isolation
 */
public class InMemoryEventStoreCount extends AbstractInMemoryEventStore
        implements Iterable<Entry<RowKey, Map<ByteArrayWrapper, MutableLong>>> {
    private final Map<RowKey, Map<ByteArrayWrapper, MutableLong>> store = new HashMap<>();

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(InMemoryEventStoreCount.class);

    private int cellCount = 0;

    public InMemoryEventStoreCount(final EventStoreMapKey eventStoreMapKey) {
        super(eventStoreMapKey);

        if (!eventStoreMapKey.getStatisticType().equals(StatisticType.COUNT)) {
            throw new IllegalArgumentException(
                    "The StatisticType in the passed map key does not match the inherent StatisticType of this class");
        }
    }

    /**
     * Increments the passed value into the in memory store
     *
     * @param cellQualifier
     *            The full rowkey and column qualfier containing the stat name,
     *            partial timestamp, tag/value pairs and column qualifier
     * @param value
     *            The value to increment the currently stored value by
     *
     * @return True if this was the first put into the store
     */
    public boolean putValue(final CellQualifier cellQualifier, final long value) {
        LOGGER.trace("putValue called for cellQualifier: {}, value: {}", cellQualifier, value);

        return putValue(cellQualifier.getRowKey(), cellQualifier.getColumnQualifier(), value);
    }

    /**
     * Increments the passed value into the in memory store
     *
     * @param rowKey
     *            The full rowkey containing the stat name, partial timestamp
     *            and tag/value pairs
     * @param columnQualifier
     *            The column qualfier as a byte array
     * @param value
     *            The value to increment the currently stored value by
     *
     * @return True if this was the first put into the store
     */
    public boolean putValue(final RowKey rowKey, final byte[] columnQualifier, final long value) {
        LOGGER.trace(() -> String.format("putValue called for rowKey: %s, columnQualifier: %s, value: %s", rowKey,
                ByteArrayUtils.byteArrayToHex(columnQualifier), value));

        final boolean result = super.isFirstDataLoadIntoStore();

        final Map<ByteArrayWrapper, MutableLong> cellsMap = store.get(rowKey);

        final ByteArrayWrapper wrappedColQual = ByteArrayWrapper.of(columnQualifier);

        if (cellsMap == null) {
            // no inner map so create one and put our value into it

            final Map<ByteArrayWrapper, MutableLong> newCellsMap = new HashMap<>();
            newCellsMap.put(wrappedColQual, new MutableLong(value));
            store.put(rowKey, newCellsMap);
            cellCount++;
        } else {
            // inner map exists so check if we have an entry for our column
            // qualifier
            final MutableLong cellValue = cellsMap.get(wrappedColQual);
            if (cellValue == null) {
                // null value could mean no key or null value, either way put a
                // new atomic long with our value
                cellsMap.put(wrappedColQual, new MutableLong(value));
                cellCount++;
            } else {
                cellValue.add(value);
            }
        }

        return result;
    }

    @Override
    public int getSize() {
        return cellCount;
    }

    @Override
    public Iterator<Entry<RowKey, Map<ByteArrayWrapper, MutableLong>>> iterator() {
        return store.entrySet().iterator();
    }
}
