

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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticType;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;

public class TestConcurrentInMemoryEventStoreValue {
    UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    private static final double JUNIT_DOUBLE_ACCEPTABLE_DELTA = 0.0001;

    @Test
    public void testPutValueSameNameAndTime() {
        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final ConcurrentInMemoryEventStoreValue store = new ConcurrentInMemoryEventStoreValue(
                getEventStoreMapKey(StatisticType.VALUE, workingInterval));

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        putCounts(store, rowKeyBuilder, 0.1, 100, 0, 1);

        assertEquals(1, store.getSize());

        for (final Entry<CellQualifier, AtomicReference<ValueCellValue>> entry : store) {
            assertEquals(10, entry.getValue().get().getAggregatedValue(), JUNIT_DOUBLE_ACCEPTABLE_DELTA);
        }
    }

    @Test
    public void testPutValueSameNameDifferentTime() {
        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final ConcurrentInMemoryEventStoreValue store = new ConcurrentInMemoryEventStoreValue(
                getEventStoreMapKey(StatisticType.VALUE, workingInterval));

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        // event every 10s so 6 events aggregated into each value in the map
        putCounts(store, rowKeyBuilder, 0.1, 120, 10_000, 1);

        assertEquals(20, store.getSize());

        for (final Entry<CellQualifier, AtomicReference<ValueCellValue>> entry : store) {
            assertEquals(0.6, entry.getValue().get().getAggregatedValue(), JUNIT_DOUBLE_ACCEPTABLE_DELTA);
        }
    }

    @Test
    public void testPutValueDifferentNameDifferentTime() {
        final EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        final ConcurrentInMemoryEventStoreValue store = new ConcurrentInMemoryEventStoreValue(
                getEventStoreMapKey(StatisticType.VALUE, workingInterval));

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        // event every 10s so 6 events aggregated into each value in the map
        putCounts(store, rowKeyBuilder, 0.1, 120, 10_000, 2);

        assertEquals(40, store.getSize());

        for (final Entry<CellQualifier, AtomicReference<ValueCellValue>> entry : store) {
            assertEquals(0.6, entry.getValue().get().getAggregatedValue(), JUNIT_DOUBLE_ACCEPTABLE_DELTA);
        }
    }

    private EventStoreMapKey getEventStoreMapKey(final StatisticType statisticType,
            final EventStoreTimeIntervalEnum intervalEnum) {
        return new EventStoreMapKey(statisticType, 1, intervalEnum, 0, TimeUnit.MILLISECONDS);
    }

    private void putCounts(final ConcurrentInMemoryEventStoreValue store, final RowKeyBuilder rowKeyBuilder,
            final double value, final int numIterations, final int timeDeltaMillis, final int namesPerIteration) {
        final String eventTimeString = "2009-01-01T00:00:00.000Z";
        long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        for (int i = 1; i <= numIterations; i++) {
            for (int j = 1; j <= namesPerIteration; j++) {
                final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent" + j, null, 1L);

                final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

                final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

                store.putValue(cellQualifiers.get(0), new ValueCellValue(value));
            }
            eventTime += timeDeltaMillis;
        }
    }
}
