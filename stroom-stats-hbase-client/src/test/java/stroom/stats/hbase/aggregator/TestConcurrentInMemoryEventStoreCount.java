

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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticType;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;

public class TestConcurrentInMemoryEventStoreCount {
    UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    @Test
    public void testPutValueSameNameAndTime() {
        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final ConcurrentInMemoryEventStoreCount store = new ConcurrentInMemoryEventStoreCount(
                getEventStoreMapKey(StatisticType.COUNT, workingInterval));

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        putCounts(store, rowKeyBuilder, 2, 15, 0, 1);

        assertEquals(1, store.getSize());

        assertAllValuesInCountStore(store, 30);
    }

    @Test
    public void testPutValueSameNameDifferentTime() {
        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final ConcurrentInMemoryEventStoreCount store = new ConcurrentInMemoryEventStoreCount(
                getEventStoreMapKey(StatisticType.COUNT, workingInterval));

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        // event every 10s so 6 events aggregated into each value in the map
        putCounts(store, rowKeyBuilder, 2, 120, 10_000, 1);

        assertEquals(20, store.getSize());

        assertAllValuesInCountStore(store, 12);
    }

    @Test
    public void testPutValueDifferentNameDifferentTime() {
        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final ConcurrentInMemoryEventStoreCount store = new ConcurrentInMemoryEventStoreCount(
                getEventStoreMapKey(StatisticType.COUNT, workingInterval));

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        // event every 10s so 6 events aggregated into each value in the map
        putCounts(store, rowKeyBuilder, 2, 120, 10_000, 2);

        assertEquals(40, store.getSize());

        assertAllValuesInCountStore(store, 12);
    }

    private EventStoreMapKey getEventStoreMapKey(final StatisticType statisticType,
            final EventStoreTimeIntervalEnum intervalEnum) {
        return new EventStoreMapKey(statisticType, 1, intervalEnum, 0, TimeUnit.MILLISECONDS);
    }

    private void putCounts(final ConcurrentInMemoryEventStoreCount store, final RowKeyBuilder rowKeyBuilder,
            final long value, final int numIterations, final int timeDeltaMillis, final int namesPerIteration) {
        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        for (int i = 1; i <= numIterations; i++) {
            for (int j = 1; j <= namesPerIteration; j++) {
                final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent" + j, null, 1L);

                final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

                final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

                store.putValue(cellQualifiers.get(0), value);
            }
            eventTime += timeDeltaMillis;
        }
    }

    private void assertAllValuesInCountStore(final ConcurrentInMemoryEventStoreCount countStore,
            final long expectedValue) {
        for (final Entry<RowKey, ConcurrentMap<ByteArrayWrapper, AtomicLong>> rowEntry : countStore) {
            for (final Entry<ByteArrayWrapper, AtomicLong> cellEntry : rowEntry.getValue().entrySet()) {
                assertEquals(expectedValue, cellEntry.getValue().longValue());
            }
        }
    }
}
