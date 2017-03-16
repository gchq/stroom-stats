

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

package stroom.stats.hbase.store.task;

import stroom.stats.hbase.aggregator.AbstractInMemoryEventStore;
import stroom.stats.hbase.aggregator.ConcurrentInMemoryEventStoreCount;
import stroom.stats.hbase.aggregator.ConcurrentInMemoryEventStoreValue;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.CountCellIncrementHolder;
import stroom.stats.hbase.structure.CountRowData;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.table.EventStoreTable;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.util.bytes.ByteArrayWrapper;
import stroom.stats.task.api.AbstractTaskHandler;
import stroom.stats.task.api.VoidResult;
import stroom.stats.util.logging.LambdaLogger;

import javax.annotation.Resource;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

//@TaskHandlerBean(task = EventStoreFlushTask.class)
//@Scope(value = StroomScope.TASK)
public class EventStoreFlushTaskHandler extends AbstractTaskHandler<EventStoreFlushTask, VoidResult> {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(EventStoreFlushTaskHandler.class);

    private int count;
    private int total;

//    @Resource
//    private TaskMonitor taskMonitor;
    @Resource
    private EventStoreTableFactory eventStoreTableFactory;

    public EventStoreFlushTaskHandler() {
    }

    @Inject
    public EventStoreFlushTaskHandler(final EventStoreTableFactory eventStoreTableFactory) {
        this.eventStoreTableFactory = eventStoreTableFactory;
    }

    @Override
    public VoidResult exec(final EventStoreFlushTask task) {
        flush(task.getMap(), task.isForcedFlushToDisk());

        return new VoidResult();
    }

    private void flush(final AbstractInMemoryEventStore store, final boolean isForcedFlushToDisk) {
        if (store != null) {
            count = 0;
            total = store.getSize();

            LOGGER.debug(() -> String.format("Flushing statistics (count=%s) for store: %s", total, store.getTimeInterval()));
//            taskMonitor.info("Flushing statistics (count=%s)", total);

            final EventStoreTable eventStoreTable = eventStoreTableFactory.getEventStoreTable(store.getTimeInterval());

            try {
                if (store instanceof ConcurrentInMemoryEventStoreCount) {
                    flushCounts((ConcurrentInMemoryEventStoreCount) store, eventStoreTable, isForcedFlushToDisk);
                } else if (store instanceof ConcurrentInMemoryEventStoreValue) {
                    flushValues((ConcurrentInMemoryEventStoreValue) store, eventStoreTable);
                } else {
                    throw new RuntimeException("Event store is an unexpected type: " + store.getClass().getName());
                }
            } catch (final Exception ex) {
                LOGGER.error("flush() - Failed flush after {} entries", count, ex);
                throw ex;
            }
        }
    }

    private void flushCounts(final ConcurrentInMemoryEventStoreCount store, final EventStoreTable eventStoreTable,
            final boolean isForcedFlushToDisk) {
        for (final CountRowData countRowData : convertCountStoreToHBaseRows(store)) {
//            if (!taskMonitor.isTerminated()) {
//                eventStoreTable.bufferedAddCount(countRowData, isForcedFlushToDisk);
//            }
        }
    }

    private void flushValues(final ConcurrentInMemoryEventStoreValue store, final EventStoreTable eventStoreTable) {
        for (final Entry<CellQualifier, AtomicReference<ValueCellValue>> entry : (store)) {
//            if (!taskMonitor.isTerminated()) {
//                eventStoreTable.addValue(entry.getKey(), entry.getValue().get());
                count++;
//            }
        }
    }

    private List<CountRowData> convertCountStoreToHBaseRows(final ConcurrentInMemoryEventStoreCount eventStore) {
        final List<CountRowData> rowList = new ArrayList<>();

        for (final Entry<RowKey, ConcurrentMap<ByteArrayWrapper, AtomicLong>> rowEntry : eventStore) {
            final List<CountCellIncrementHolder> cells = new ArrayList<>();

            for (final Entry<ByteArrayWrapper, AtomicLong> cellEntry : rowEntry.getValue().entrySet()) {
                cells.add(new CountCellIncrementHolder(cellEntry.getKey().getBytes(), cellEntry.getValue().get()));
            }
            rowList.add(new CountRowData(rowEntry.getKey(), cells));
        }

        return rowList;
    }
}
