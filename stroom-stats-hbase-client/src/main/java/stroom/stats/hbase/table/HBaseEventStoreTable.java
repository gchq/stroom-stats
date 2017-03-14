

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

package stroom.stats.hbase.table;

import javaslang.Tuple2;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.Period;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.exception.HBaseException;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.CountCellIncrementHolder;
import stroom.stats.hbase.structure.CountRowData;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.table.filter.StatisticsTagValueFilter;
import stroom.stats.hbase.table.filter.TagValueFilterTreeBuilder;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.task.api.TaskManager;
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class HBaseEventStoreTable extends HBaseTable implements EventStoreTable {
    private final String displayName;
    private final TableName tableName;
    private final EventStoreTimeIntervalEnum timeInterval;
    private final TaskManager taskManager;
    private final StroomPropertyService propertyService;
    private final HBaseCountPutBuffer countPutBuffer;
    //TODO don't know why this is lazily initialised, just do it eagerly in the ctor
    private final RowKeyBuilder rowKeyBuilder;

    private static final String DISPLAY_NAME_POSTFIX = " EventStore";
    public static final String TABLE_NAME_POSTFIX = "es";

    // private ScheduledExecutorService debugSnapshotScheduler;

    // static variable so we have a set of permits that work accross all event
    // store intervals
    private static Semaphore maxConcurrentBatchPutTasksSemaphore = null;
    private static final Object SEMAPHORE_LOCK_OBJECT = new Object();

    // counters to track how many cell puts we do for the
    private final LongAdder cellPutCounterCount = new LongAdder();
    private final LongAdder cellPutCounterValue = new LongAdder();

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseEventStoreTable.class);

    /**
     * Private constructor
     */
    private HBaseEventStoreTable(final EventStoreTimeIntervalEnum eventStoreTimeIntervalEnum,
            final TaskManager taskManager, final StroomPropertyService propertyService,
            final HBaseConnection hBaseConnection, final UniqueIdCache uniqueIdCache) {
        super(hBaseConnection);
        this.displayName = eventStoreTimeIntervalEnum.longName() + DISPLAY_NAME_POSTFIX;
        this.tableName = TableName.valueOf(Bytes.toBytes(eventStoreTimeIntervalEnum.shortName() + TABLE_NAME_POSTFIX));
        this.timeInterval = eventStoreTimeIntervalEnum;
        this.taskManager = taskManager;
        this.propertyService = propertyService;
        this.rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, timeInterval);

        countPutBuffer = new HBaseCountPutBuffer(getPutBufferSize());

        initMaxBatchPutTasksSemaphore();

        init();
    }

    private void initMaxBatchPutTasksSemaphore() {
        synchronized (SEMAPHORE_LOCK_OBJECT) {
            if (maxConcurrentBatchPutTasksSemaphore == null) {
                maxConcurrentBatchPutTasksSemaphore = new Semaphore(getMaxConcurrentBatchPutTasks());
            }
        }
    }

    /**
     * Static constructor
     */
    public static HBaseEventStoreTable getInstance(final EventStoreTimeIntervalEnum eventStoreTimeIntervalEnum,
            final TaskManager taskManager, final StroomPropertyService propertyService,
            final HBaseConnection hBaseConnection, final UniqueIdCache uniqueIdCache) {
        return new HBaseEventStoreTable(eventStoreTimeIntervalEnum, taskManager, propertyService, hBaseConnection, uniqueIdCache);
    }


    @Override
    public void addAggregatedEvents(final StatisticType statisticType, final List<AggregatedEvent> aggregatedEvents) {

        switch (statisticType) {
            case COUNT:
                putAggregatedEventsCount(aggregatedEvents);
                break;
            case VALUE:
                putAggregatedEventsValue(aggregatedEvents);
                break;
            default:
                throw new IllegalArgumentException("Unexpected statisticType " + statisticType);
        }
    }

    private void putAggregatedEventsCount(final List<AggregatedEvent> aggregatedEvents) {

        LOGGER.trace(() -> String.format("putAggregatedEventsCount called with size %s", aggregatedEvents.size()));

        //Convert the aggregated events into row keys and cell qualifiers then
        //group them by rowkey so we have a list of cell increments for each row
        //meaning hbase can lock a row and make multiple changes to it at once.
        //Tuple2 is (RowKey, CountCellIncrementHolder)
        Map<RowKey, List<CountCellIncrementHolder>> rowData = aggregatedEvents.stream()
                .map(aggregatedEvent -> {
                    CellQualifier cellQualifier = rowKeyBuilder.buildCellQualifier(aggregatedEvent);
                    long countIncrement = ((CountAggregate) aggregatedEvent.getStatAggregate()).getAggregatedCount();
                    return new Tuple2<>(
                            cellQualifier.getRowKey(),
                            new CountCellIncrementHolder(cellQualifier.getColumnQualifier(), countIncrement));
                })
                .collect(Collectors.groupingBy(
                        Tuple2::_1, Collectors.mapping(Tuple2::_2, Collectors.toList())));

        addMultipleCounts(rowData);
    }

    private void putAggregatedEventsValue(final List<AggregatedEvent> aggregatedEvents) {

        LOGGER.trace(() -> String.format("putAggregatedEventsValue called with size %s", aggregatedEvents.size()));
        //TODO ValueCellValue and ValueAggregate are essentially the same thing. Should probably keep Value Aggregate
        //and put any additional code from VCV into it.
        aggregatedEvents.forEach(aggregatedEvent -> {
            CellQualifier cellQualifier = rowKeyBuilder.buildCellQualifier(aggregatedEvent);
            ValueAggregate valueAggregate;
            try {
                valueAggregate = (ValueAggregate) aggregatedEvent.getStatAggregate();
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("StatAggregate %s is of the wrong type",
                        aggregatedEvent.getStatAggregate().getClass().getName()));
            }
            ValueCellValue valueCellValue = new ValueCellValue(
                    valueAggregate.getCount(),
                    valueAggregate.getAggregatedValue(),
                    valueAggregate.getMinValue(),
                    valueAggregate.getMaxValue());

            addValue(cellQualifier, valueCellValue);
        });
    }

//    @Override
//    public int getPutBufferCount(final boolean isDeepCount) {
//        return isDeepCount ? countPutBuffer.getCellCount() : countPutBuffer.getQueueSize();
//    }

//    @Override
//    public int getAvailableBatchPutTaskPermits() {
//        return HBaseEventStoreTable.maxConcurrentBatchPutTasksSemaphore.availablePermits();
//    }

    @Override
    public long getCellsPutCount(final StatisticType statisticType) {
        return StatisticType.COUNT.equals(statisticType) ? cellPutCounterCount.sum() : cellPutCounterValue.sum();
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public HTableDescriptor getDesc() {
        final HTableDescriptor desc = new HTableDescriptor(getName());

        final HColumnDescriptor countsColDesc = new HColumnDescriptor(EventStoreColumnFamily.COUNTS.asByteArray());
        countsColDesc.setMaxVersions(1);

        final HColumnDescriptor valuesColDesc = new HColumnDescriptor(EventStoreColumnFamily.VALUES.asByteArray());
        valuesColDesc.setMaxVersions(1);
        desc.addFamily(countsColDesc);
        desc.addFamily(valuesColDesc);
        return desc;
    }

//    @Override
//    public void bufferedAddCount(final CountRowData countRowData, final boolean isForcedFlushToDisk) {
//        // try {
//        // LOGGER.info("bufferedAddCount called for [%s] cells, waiting
//        // forever", countRowData.getCells().size());
//        // new Semaphore(0).acquire();
//        // } catch (InterruptedException e) {
//        // LOGGER.info("interupted");
//        // }
//
//        LOGGER.trace(() -> String.format("bufferedAddCount called for interval: %s, rowKey: %s, cell count: %s", this.timeInterval,
//                countRowData.getRowKey().toString(), countRowData.getCells().size()));
//
//        try {
//            // queue will block its puts when it reaches its limit
//            countPutBuffer.put(countRowData);
//        } catch (final InterruptedException e1) {
//            // something has interrupted this thread while it was waiting to do
//            // the put
//            // re-enable the interrupt status
//            Thread.currentThread().interrupt();
//        }
//
//        LOGGER.trace(() -> String.format("countPutBuffer size: %s, cell count: %s", countPutBuffer.getQueueSize(),
//                countPutBuffer.getCellCount()));
//
//        processBatchFromQueue(isForcedFlushToDisk);
//    }

//    /**
//     * If the deep cell count of the countPutBuffer has reach a certain point it
//     * will attempt to repeatedly take items until it has a desired number. The
//     * taken items are then sent on for persistence to HBase
//     *
//     * @param isForcedFlushToDisk
//     *            true if you want it to flush everything to HBase regardless of
//     *            how much is in the queue
//     */
//    private void processBatchFromQueue(final boolean isForcedFlushToDisk) {
//        final List<CountRowData> rowsFromBuffer = new ArrayList<>();
//        final int putBufferTakeCount = getPutTakeCount();
//
//        int cumulativeCellCount = 0;
//        CountRowData rowFromQueue = null;
//
//        if (countPutBuffer.getCellCount() >= putBufferTakeCount || isForcedFlushToDisk)
//            do {
//                // grab one row's worth of data from the queue
//                rowFromQueue = countPutBuffer.poll();
//
//                if (rowFromQueue != null) {
//                    rowsFromBuffer.add(rowFromQueue);
//                    cumulativeCellCount += rowFromQueue.getCells().size();
//                }
//            } while (rowFromQueue != null && cumulativeCellCount <= putBufferTakeCount);
//
//        if (rowsFromBuffer.size() > 0) {
//            createMultiplePutsTask(rowsFromBuffer);
//        }
//    }

//    //TODO do we realy need to do this rather than just firing increments at the tableinterface and
//    //letting it buffer them?
//    /**
//     * Create an async task to persist a batch of count stats
//     */
//    private void createMultiplePutsTask(final List<CountRowData> rowsFromBuffer) {
//        if (rowsFromBuffer != null && rowsFromBuffer.size() > 0) {
//            final HBaseBatchPutTask task = new HBaseBatchPutTask(timeInterval, rowsFromBuffer);
//
//            LOGGER.debug(() -> String.format("About to get permit for batch put task with row count: %s.  Available permits: %s, max permits: %s",
//                    rowsFromBuffer.size(), maxConcurrentBatchPutTasksSemaphore.availablePermits(),
//                    getMaxConcurrentBatchPutTasks()));
//
//            try {
//                maxConcurrentBatchPutTasksSemaphore.acquire();
//            } catch (final InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//            try {
//                taskManager.execAsync(task, new TaskCallbackAdaptor<VoidResult>() {
//                    @Override
//                    public void onSuccess(final VoidResult result) {
//                        maxConcurrentBatchPutTasksSemaphore.release();
//                        LOGGER.trace("EventStoreFlushTask completed successfully for store: {}", timeInterval);
//                    }
//
//                    @Override
//                    public void onFailure(final Throwable t) {
//                        maxConcurrentBatchPutTasksSemaphore.release();
//                        LOGGER.error("EventStoreFlushTask failed for store: {}", timeInterval, t);
//
//                    };
//                });
//            } catch (final Exception e) {
//                maxConcurrentBatchPutTasksSemaphore.release();
//            }
//        }
//    }

    private void addMultipleCounts(final Map<RowKey, List<CountCellIncrementHolder>> rowChanges) {
        LOGGER.trace(() -> String.format("addMultipleCounts called for %s rows", rowChanges.size()));

        // create an action for each row we have data for
        final List<Mutation> actions = rowChanges.entrySet().stream()
                .map(entry -> createIncrementOperation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        final Object[] results = null;
        // don't care about what is written to results as we are doing puts send the mutations to HBase

        // long startTime = System.currentTimeMillis();
        doBatch(actions, results);
        cellPutCounterCount.add(actions.size());
        LOGGER.trace(() -> String.format("%s puts sent to HBase", actions.size()));

        // LOGGER.info("Sent %s ADDs to HBase from thread %s in %s ms",
        // cellQualifiersFromBuffer.size(),
        // Thread.currentThread().getName(), (System.currentTimeMillis() -
        // startTime));

    }

//    @Override
//    public void flushPutBuffer() {
//        LOGGER.debug("flushPutBuffer called for store: {}", timeInterval);
//
//        // keep processing batches from the queue until it is empty
//        while (countPutBuffer.getQueueSize() > 0) {
//            processBatchFromQueue(true);
//        }
//    }

    /**
     * Builds a single {@link Increment} object for a row, with one-many cell
     * increments in that row
     *
     * @param rowKey
     *            The rowKey of the row to be updated
     * @param cells
     *            A list of objects containing the column qualifier and cell
     *            increment value
     * @return The completed {@link Increment} object
     */
    private Increment createIncrementOperation(final RowKey rowKey, final List<CountCellIncrementHolder> cells) {
        LOGGER.trace(() -> String.format("createIncrementOperation called for rowKey: %s with cell count %s", rowKey.toString(),
                cells.size()));

        final Increment increment = new Increment(rowKey.asByteArray());

        // TODO HBase 2.0 has Increment.setReturnResults to allow you to prevent
        // the return of the new
        // value to improve performance. In our case we don't care about the new
        // value so when we
        // upgrade to HBase 2.0 we need to add this line in.
        // increment.setReturnResults(false);

        for (final CountCellIncrementHolder cell : cells) {
            increment.addColumn(EventStoreColumnFamily.COUNTS.asByteArray(), cell.getColumnQualifier(),
                    cell.getCellIncrementValue());
        }
        return increment;
    }

    private void addValue(final CellQualifier cellQualifier, final ValueCellValue valueCellValue) {
        //TODO need some means of handling the event identifiers, possibly use the HBase append method to keep
        //adding ID into a cell (in another col fam), however this would make it difficult to control the number of ids
        //being put into the cell, though maybe that doesn't matter if we try and limit a bit during streams aggregation.
        //An alternative is to use a coprocessor to handle the checkAndPut of the ValueCellValue and the identifiers in one go,
        //thus saving any round trips.
        LOGGER.trace("addValue called for cellQualifier: {}, valueCellValue: {}", cellQualifier, valueCellValue);

        final int maxAttemptsAtCheckAndPut = getCheckAndPutRetryCount();

        // we cannot buffer the adding of values due to the two step get-and-set
        // nature of it. This makes value
        // statistics much more expensive than count statistics

        int retryCounter = maxAttemptsAtCheckAndPut;
        boolean hasPutSucceeded = false;

        final Table tableInterface = getTable();

        try {
            // loop in case another thread beats us to the checkAndPut
            while (retryCounter-- > 0) {
                // get the current value of the cell
                final ValueCellValue currCellValue = new ValueCellValue(
                        getCellValue(tableInterface, cellQualifier, EventStoreColumnFamily.VALUES.asByteArray()));

                // aggregate the new value into the existing cell, incrementing
                // the count and working out the max/min
                final ValueCellValue newCellValue = currCellValue.addAggregatedValues(valueCellValue);

                // construct the put containing the new aggregated cell value
                final Put put = new Put(cellQualifier.getRowKey().asByteArray()).addColumn(
                        EventStoreColumnFamily.VALUES.asByteArray(), cellQualifier.getColumnQualifier(),
                        newCellValue.asByteArray());

                // do a check and put - atomic operation to only do the put if
                // the cell value still looks like
                // currCellValue. If it fails go round again for another go
                hasPutSucceeded = doCheckAndPut(tableInterface, cellQualifier.getRowKey().asByteArray(),
                        EventStoreColumnFamily.VALUES.asByteArray(), cellQualifier.getColumnQualifier(),
                        (currCellValue.isEmpty() ? null : currCellValue.asByteArray()), put);

                if (hasPutSucceeded) {
                    // put worked so no need to retry
                    cellPutCounterValue.increment();
                    break;
                } else {
                    LOGGER.trace("CheckAndPut failed on retry {}, retrying", retryCounter);
                }
            }
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        } finally {
            closeTable(tableInterface);
        }

        if (!hasPutSucceeded) {
            throw new RuntimeException(
                    "Put operation failed after [" + maxAttemptsAtCheckAndPut + "] retries for cellQualifier ["
                            + cellQualifier + "] and valueCellValue [" + valueCellValue + "]");
        }
    }

    @Override
    public StatisticDataSet getStatisticsData(final UniqueIdCache uniqueIdCache, final StatisticConfiguration dataSource,
            final RollUpBitMask rollUpBitMask, final FindEventCriteria criteria) {
        LOGGER.debug("getChartData called for criteria: {}", criteria);

        final Period period = criteria.getPeriod();

        final StatisticType statisticType = dataSource.getStatisticType();

        LOGGER.debug(() -> String.format("getChartData() called on eventStore: [%s]", timeInterval.longName()));
        LOGGER.debug(() -> String.format("Using time period: [%s] to [%s]", DateUtil.createNormalDateTimeString(period.getFrom()),
                DateUtil.createNormalDateTimeString(period.getTo())));

        final RowKey startRowKey = rowKeyBuilder.buildStartKey(criteria.getStatisticName(), rollUpBitMask,
                criteria.getPeriod().getFrom());
        // need to subtract one from the period to time as it is exclusive and
        // we want to work from an inclusive
        // time
        final RowKey endRowKeyExclusive = rowKeyBuilder.buildEndKey(criteria.getStatisticName(), rollUpBitMask,
                criteria.getPeriod().getTo() - 1L);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("startRowKey: " + rowKeyBuilder.toPlainTextString(startRowKey));
            LOGGER.debug("startRowKey: " + ByteArrayUtils.byteArrayToHex(startRowKey.asByteArray()));
            LOGGER.debug("endRowKey:   " + rowKeyBuilder.toPlainTextString(endRowKeyExclusive));
            LOGGER.debug("endRowKey:   " + ByteArrayUtils.byteArrayToHex(endRowKeyExclusive.asByteArray()));
        }

        // Query the event store over the given range.
        // We may only want part of the row from the first row and last row
        // but will want all columns from the rest of the rows. This is
        // because the start/end time may not land exactly on the row key
        // time interval boundaries.

        // TODO also may need to set decent values for setBatch and
        // setCaching to ensure good performance

        final Scan scan = new Scan(startRowKey.asByteArray(), endRowKeyExclusive.asByteArray());
        scan.setMaxVersions(1);
        scan.setCaching(1_000);

        // determine which column family to use based on the kind of data we are
        // trying to query
        if (statisticType.equals(StatisticType.COUNT)) {
            scan.addFamily(EventStoreColumnFamily.COUNTS.asByteArray());
        } else {
            scan.addFamily(EventStoreColumnFamily.VALUES.asByteArray());
        }

        final Table tableInterface = getTable();

        addScanFilter(scan, criteria, uniqueIdCache);

        final ResultScanner scanner = getScanner(tableInterface, scan);

        // object to hold all the data returned
        final StatisticDataSet statisticDataSet = new StatisticDataSet(criteria.getStatisticName(), statisticType);

        try {
            final long periodFrom = period.getFrom();
            final long periodTo = period.getTo();

            // loop through each row in the result set from the scan, so
            // this is all rows
            // for that time period (using partial timestamps) and UID
            for (final Result result : scanner) {
                // attempt to build an object from the raw row key bytes
                final RowKey rowKeyObject = new RowKey(result.getRow());

                final List<StatisticTag> tags = rowKeyBuilder.getTagValuePairsAsList(rowKeyObject);

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Row key: " + rowKeyBuilder.toPlainTextString(rowKeyObject));
                    LOGGER.trace("Row key: " + ByteArrayUtils.byteArrayToString(rowKeyObject.asByteArray()));
                }

                final CellScanner cellScanner = result.cellScanner();
                // loop through each cell in the row
                // each cell has a column qualifier that is the interval
                // number in the row interval
                // e.g. if the row interval is hourly then the 5s value will
                // have a column qualifier of 5 and there will be up to 3600
                // columns
                while (cellScanner.advance()) {
                    final Cell cell = cellScanner.current();

                    // get the column qualifier
                    final byte[] bTimeQualifier = new byte[cell.getQualifierLength()];

                    // need the array copy as we will hold the col qual as a
                    // byte[] on the CellQualifier object
                    System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), bTimeQualifier, 0,
                            cell.getQualifierLength());

                    final CellQualifier cellQualifier = rowKeyBuilder.buildCellQualifier(rowKeyObject,
                            bTimeQualifier);

                    final long fullTimestamp = cellQualifier.getFullTimestamp();

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("ColQualBytes: " + ByteArrayUtils.byteArrayToString(bTimeQualifier)
                                + " ColQualInt: " + Bytes.toInt(bTimeQualifier) + " FullTimestamp: "
                                + DateUtil.createNormalDateTimeString(fullTimestamp));
                    }

                    // filter the cell to ensure it is in the period we are
                    // after
                    if (fullTimestamp >= periodFrom && fullTimestamp <= periodTo) {
                        final StatisticDataPoint dataPoint;

                        if (statisticType.equals(StatisticType.COUNT)) {
                            dataPoint = buildCountDataPoint(fullTimestamp, tags, cell.getValueArray(),
                                    cell.getValueOffset(), cell.getValueLength());
                        } else {
                            dataPoint = buildValueDataPoint(fullTimestamp, tags, cell.getValueArray(),
                                    cell.getValueOffset(), cell.getValueLength());
                        }

                        statisticDataSet.addDataPoint(dataPoint);

                    } else if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Ignoring value as it is outside the time period");
                    }
                }
            }
        } catch (final Throwable t) {
            closeScanner(scanner);
            closeTable(tableInterface);
            throw new HBaseException(t.getMessage(), t);
        } finally {
            closeScanner(scanner);
            closeTable(tableInterface);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(() -> String.format("Found %s data points", statisticDataSet.size()));
        }

        return statisticDataSet;
    }

    /**
     * @param fullTimestamp
     * @param tags
     * @param bytes
     *            An array of bytes which contains the cell value within it
     * @param cellValueOffset
     *            The start position of the cell value within bytes
     * @param cellValueLength
     *            The length of the cell value within bytes
     * @return
     */
    private StatisticDataPoint buildCountDataPoint(final long fullTimestamp, final List<StatisticTag> tags,
            final byte[] bytes, final int cellValueOffset, final int cellValueLength) {
        final long count = (cellValueLength == 0) ? 0 : Bytes.toLong(bytes, cellValueOffset, cellValueLength);

        return StatisticDataPoint.countInstance(fullTimestamp, timeInterval.columnInterval(), tags, count);
    }

    /**
     * @param fullTimestamp
     * @param tags
     * @param bytes
     *            An array of bytes which contains the cell value within it
     * @param cellValueOffset
     *            The start position of the cell value within bytes
     * @param cellValueLength
     *            The length of the cell value within bytes
     * @return
     */
    private StatisticDataPoint buildValueDataPoint(final long fullTimestamp, final List<StatisticTag> tags,
            final byte[] bytes, final int cellValueOffset, final int cellValueLength) {
        final ValueCellValue cellValue = new ValueCellValue(bytes, cellValueOffset, cellValueLength);

        return StatisticDataPoint.valueInstance(fullTimestamp, timeInterval.columnInterval(), tags,
                cellValue.getAverageValue(), cellValue.getCount(), cellValue.getMinValue(), cellValue.getMaxValue());
    }

    @Override
    public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                                      final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask, final Period period) {
        boolean isFound = false;
        final String statName = statisticConfiguration.getName();

        final byte[] statNameUid = uniqueIdCache.getUniqueIdOrDefault(statName).getUidBytes();

        final Scan scan = new Scan();

        if (period.getFrom() != null) {
            final RowKey startRowKey = rowKeyBuilder.buildStartKey(statisticConfiguration.getName(), rollUpBitMask,
                    period.getFrom());
            scan.setStartRow(startRowKey.asByteArray());
        }

        if (period.getTo() != null) {
            // need to subtract one from the period to time as it is exclusive
            // and we want to work from an inclusive
            // time
            final RowKey endRowKeyExclusive = rowKeyBuilder.buildEndKey(statName, rollUpBitMask,
                    period.getTo() - 1L);
            scan.setStopRow(endRowKeyExclusive.asByteArray());
        }

        // filter on rows with a key starting with the UID of our stat name
        final Filter prefixFilter = new PrefixFilter(statNameUid);

        // filter on the first row found
        final Filter pageFilter = new PageFilter(1);

        final Filter keyOnlyFilter = new KeyOnlyFilter();

        final FilterList filters = new FilterList();
        filters.addFilter(prefixFilter);
        filters.addFilter(pageFilter);
        filters.addFilter(keyOnlyFilter);

        scan.setFilter(filters);

        final Table tableInterface = getTable();
        final ResultScanner scanner = getScanner(tableInterface, scan);

        try {
            // the page filter may return more than one row as it is run on each
            // region so you may get one per
            // region.
            // we just want the first one we find

            final Result result = scanner.next();

            if (result != null && result.getRow() != null) {
                isFound = true;
            }

        } catch (final Throwable t) {
            closeScanner(scanner);
            closeTable(tableInterface);
            throw new HBaseException(t.getMessage(), t);
        } finally {
            closeScanner(scanner);
            closeTable(tableInterface);
        }

        return isFound;

    }

    @Override
    public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
            final StatisticConfiguration statisticConfiguration) {
        // call the overloaded method with a period with no from/to
        return doesStatisticExist(uniqueIdCache, statisticConfiguration, null, new Period(null, null));

    }

    /**
     * Create a {@link StatisticsTagValueFilter} from the filter tree in the criteria.
     * Adds the filter to the scan object
     *
     * @param scan
     *            The HBase scan object being used to scan the rows
     * @param criteria
     *            The search criteria from the UI
     * @param uniqueIdCache
     *            The UID cache to convert strings into the UIDs understood by
     *            HBase
     */
    private void addScanFilter(final Scan scan, final FindEventCriteria criteria, final UniqueIdCache uniqueIdCache) {
        // criteria may not have a filterTree on it
        if (!criteria.getFilterTermsTree().equals(FilterTermsTree.emptyTree())) {
            Filter tagValueFilter = null;

            tagValueFilter = new StatisticsTagValueFilter(
                    TagValueFilterTreeBuilder.buildTagValueFilterTree(criteria.getFilterTermsTree(), uniqueIdCache));

            scan.setFilter(tagValueFilter);
        }
    }

    private byte[] getCellValue(final Table tableInterface, final CellQualifier cellQualifier,
            final byte[] columnFamily) throws IOException {
        final Get get = new Get(cellQualifier.getRowKey().asByteArray());
        get.addColumn(columnFamily, cellQualifier.getColumnQualifier());

        final Result result = doGet(tableInterface, get);

        return result.getValue(columnFamily, cellQualifier.getColumnQualifier());
    }

    /**
     * Intended to be called by another class as this class is not a spring bean
     * and therefore cannot use the Stroom Lifecycle annotations
     */
    @Override
    public void shutdown() {
//        flushPutBuffer();

    }

    private int getPutBufferSize() {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.DATA_STORE_PUT_BUFFER_MAX_SIZE_PROPERTY_NAME);
    }

    private int getPutTakeCount() {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.DATA_STORE_PUT_BUFFER_TAKE_COUNT_PROPERTY_NAME);
    }

    private int getCheckAndPutRetryCount() {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.DATA_STORE_MAX_CHECK_AND_PUT_RETRIES_PROPERTY_NAME);
    }

    private int getMaxConcurrentBatchPutTasks() {
        return propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.DATA_STORE_PURGE_MAX_BATCH_PUT_TASKS);
    }

    @Override
    public void purgeUntilTime(final UniqueIdCache uniqueIdCache,
                               final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask,
                               final long purgeUpToTimeMs) {
        final long startTime = System.currentTimeMillis();

        if (purgeUpToTimeMs > System.currentTimeMillis()) {
            throw new RuntimeException(
                    String.format("PurgeStatStore called with a time [%s] in the future.", purgeUpToTimeMs));
        }

        final String statisticName = statisticConfiguration.getName();

        final RowKey startRowKey = rowKeyBuilder.buildStartKey(statisticName, rollUpBitMask, 0);

        // take one milli off the purge up to time as we want everything before
        // it. e.g.
        // time now is 08:05:07
        // time rounded to row key interval (second store) is 08:00:00
        // purge retention is 2 intervals so
        // purgeUpToTime is 06:00:00
        // subtract 1milli is 05:59:59.999
        // end key should then be 06:00:00
        final RowKey endRowKeyExclusive = rowKeyBuilder.buildEndKey(statisticName, rollUpBitMask,
                purgeUpToTimeMs - 1L);

        LOGGER.debug("Using start key:       " + rowKeyBuilder.toPlainTextString(startRowKey));
        LOGGER.debug("Using end key (excl.): " + rowKeyBuilder.toPlainTextString(endRowKeyExclusive));

        final Scan scan = new Scan(startRowKey.asByteArray(), endRowKeyExclusive.asByteArray());
        scan.setMaxVersions(1);

        // TODO make this a global prop
        scan.setCaching(1_000);

        // add a keyOnlyFilter so we only get back the keys and not the data
        final FilterList filters = new FilterList();
        filters.addFilter(new KeyOnlyFilter());
        scan.setFilter(filters);

        int rowCount = 0;
        byte[] minRowKey = null;
        byte[] maxRowKey = null;

        final Table tableInterface = getTable();
        final ResultScanner scanner = getScanner(tableInterface, scan);

        try {
            Result[] results;

            // get a batch of 1000 row keys from the store, add each one to a
            // list then tell HBase to delete the
            // rows for each. A row can represent a long period of time, e.g.
            // for the second store a row represents
            // a whole hour
            do {
                results = scanner.next(1_000);

                if (results != null && results.length > 0) {
                    final List<Delete> deletes = new ArrayList<>();

                    for (final Result result : results) {
                        if (result != null) {
                            if (minRowKey == null || Bytes.compareTo(result.getRow(), minRowKey) < 0) {
                                minRowKey = result.getRow();
                            }
                            if (maxRowKey == null || Bytes.compareTo(result.getRow(), maxRowKey) > 0) {
                                maxRowKey = result.getRow();
                            }

                            deletes.add(new Delete(result.getRow()));
                            rowCount++;
                        }
                    }

                    if (deletes.size() > 0) {
                        tableInterface.delete(deletes);
                    }
                }
            } while (results != null && results.length > 0);

        } catch (final Throwable t) {
            closeScanner(scanner);
            closeTable(tableInterface);
            throw new HBaseException(t.getMessage(), t);
        } finally {
            closeScanner(scanner);
            closeTable(tableInterface);
        }

        final long runTime = System.currentTimeMillis() - startTime;

        // convert the min and max row key arrays into row key objects and then
        // convert that to the timestamp of the row
        final String minDate = minRowKey != null
                ? DateUtil.createNormalDateTimeString(rowKeyBuilder.getPartialTimestamp(new RowKey(minRowKey)))
                : "n/a";

        String maxDate;

        if (maxRowKey != null) {
            long endTime = rowKeyBuilder.getPartialTimestamp(new RowKey(maxRowKey));

            // a row has a time period equal to the row key interval and the row
            // key time is the start of that interval
            // so add a whole interval and then subtract 1ms to get the end
            // time.
            endTime = endTime + timeInterval.rowKeyInterval() - 1L;

            maxDate = DateUtil.createNormalDateTimeString(endTime);
        } else {
            maxDate = "n/a";
        }

        if (rowCount > 0) {
            int finalRowCount = rowCount;
            LOGGER.info(() -> String.format(
                    "Purged [%s] rows with time range [%s - %s] for statistic [%s] with mask [%s] in store [%s] in %.2fs",
                    finalRowCount, minDate, maxDate, statisticName, rollUpBitMask.asHexString(), timeInterval.longName(),
                    (double) runTime / (double) 1000));
        }

    }

    @Override
    public void purgeAll(final UniqueIdCache uniqueIdCache,
                               final StatisticConfiguration statisticConfiguration) {
        final long startTime = System.currentTimeMillis();

        final String statisticName = statisticConfiguration.getName();

        final byte[] bRowKey = uniqueIdCache.getUniqueId(statisticName)
                .getOrElseThrow(() -> new RuntimeException("No UID found for statistic " + statisticName))
                .getUidBytes();

        // add a keyOnlyFilter so we only get back the keys and not the data
        final FilterList filters = new FilterList();
        filters.addFilter(new KeyOnlyFilter());

        final Scan scan = new Scan()
                .setRowPrefixFilter(bRowKey)
                .setMaxVersions(1)
                .setFilter(filters);

        // TODO make this a global prop
        scan.setCaching(1_000);

        int rowCount = 0;
        final Table tableInterface = getTable();
        final ResultScanner scanner = getScanner(tableInterface, scan);

        try {
            Result[] results;

            // get a batch of 1000 row keys from the store, add each one to a
            // list then tell HBase to delete the
            // rows for each.
            do {
                results = scanner.next(1_000);

                if (results != null && results.length > 0) {
                    final List<Delete> deletes = new ArrayList<>();

                    for (final Result result : results) {
                        if (result != null) {
                            deletes.add(new Delete(result.getRow()));
                            rowCount++;
                        }
                    }

                    if (deletes.size() > 0) {
                        tableInterface.delete(deletes);
                    }
                }
            } while (results != null && results.length > 0);

        } catch (final Throwable t) {
            closeScanner(scanner);
            closeTable(tableInterface);
            throw new HBaseException(t.getMessage(), t);
        } finally {
            closeScanner(scanner);
            closeTable(tableInterface);
        }

        final long runTime = System.currentTimeMillis() - startTime;

        if (rowCount > 0) {
            int finalRowCount = rowCount;
            LOGGER.info(() -> String.format(
                    "Purged [%s] rows for statistic [%s] in store [%s] in %.2fs",
                    finalRowCount, statisticName, timeInterval.longName(),
                    (double) runTime / (double) 1000));
        }
    }

    /**
     * Class to decorate a BlockingQueue such that we can keep track of the
     * number of cell values in all of the queue entires, e.g. we may only have
     * one entry in the queue but that entry may contain thousands of values.
     */
    private static class HBaseCountPutBuffer {
        private final BlockingQueue<CountRowData> countPutBuffer;

        private final AtomicInteger cellCount = new AtomicInteger(0);

        public HBaseCountPutBuffer(final int queueSize) {
            this.countPutBuffer = new LinkedBlockingQueue<>(queueSize);
        }

        public int getQueueSize() {
            return countPutBuffer.size();
        }

        public int getCellCount() {
            return cellCount.get();
        }

        public void put(final CountRowData row) throws InterruptedException {
            countPutBuffer.put(row);
            cellCount.addAndGet(row.getCells().size());
        }

        public CountRowData poll() {
            final CountRowData countRowData = countPutBuffer.poll();
            if (countRowData != null) {
                cellCount.addAndGet(-countRowData.getCells().size());
            }
            return countRowData;
        }

        @Override
        public String toString() {
            return "HBaseCountPutBuffer [countPutBuffer count=" + countPutBuffer.size() + ", cellCount=" + cellCount
                    + "]";
        }
    }
}
