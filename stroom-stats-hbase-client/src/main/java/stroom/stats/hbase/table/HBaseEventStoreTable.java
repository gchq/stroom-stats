

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
import stroom.stats.common.Period;
import stroom.stats.common.SearchStatisticsCriteria;
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
import stroom.stats.hbase.structure.ColumnQualifier;
import stroom.stats.hbase.structure.CountCellIncrementHolder;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.StatisticDataPointAdapter;
import stroom.stats.hbase.structure.StatisticDataPointAdapterFactory;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.table.filter.StatisticsTagValueFilter;
import stroom.stats.hbase.table.filter.TagValueFilterTreeBuilder;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class HBaseEventStoreTable extends HBaseTable implements EventStoreTable {
    private final String displayName;
    private final TableName tableName;
    private final EventStoreTimeIntervalEnum timeInterval;
    private final StroomPropertyService propertyService;
    private final RowKeyBuilder rowKeyBuilder;
    private final StatisticDataPointAdapterFactory statisticDataPointAdapterFactory;

    private static final String DISPLAY_NAME_POSTFIX = " EventStore";
    public static final String TABLE_NAME_POSTFIX = "es";


    // counters to track how many cell puts we do for the
    private final Map<StatisticType, LongAdder> putCounterMap = new EnumMap<>(StatisticType.class);

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseEventStoreTable.class);

//    private Map<StatEventKey, StatAggregate> putEventsMap = new HashMap<>();

    /**
     * Private constructor
     */
    private HBaseEventStoreTable(final EventStoreTimeIntervalEnum eventStoreTimeIntervalEnum,
                                 final StroomPropertyService propertyService,
                                 final HBaseConnection hBaseConnection,
                                 final UniqueIdCache uniqueIdCache,
                                 final StatisticDataPointAdapterFactory statisticDataPointAdapterFactory) {
        super(hBaseConnection);
        this.displayName = eventStoreTimeIntervalEnum.longName() + DISPLAY_NAME_POSTFIX;
        this.tableName = TableName.valueOf(Bytes.toBytes(eventStoreTimeIntervalEnum.shortName() + TABLE_NAME_POSTFIX));
        this.timeInterval = eventStoreTimeIntervalEnum;
        this.propertyService = propertyService;
        this.rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, timeInterval);
        this.statisticDataPointAdapterFactory = statisticDataPointAdapterFactory;

        for (StatisticType statisticType : StatisticType.values()) {
            putCounterMap.put(statisticType, new LongAdder());
        }

        init();
    }


    /**
     * Static constructor
     */
    public static HBaseEventStoreTable getInstance(final EventStoreTimeIntervalEnum eventStoreTimeIntervalEnum,
                                                   final StroomPropertyService propertyService,
                                                   final HBaseConnection hBaseConnection,
                                                   final UniqueIdCache uniqueIdCache,
                                                   final StatisticDataPointAdapterFactory statisticDataPointAdapterFactory) {

        return new HBaseEventStoreTable(eventStoreTimeIntervalEnum,
                propertyService,
                hBaseConnection,
                uniqueIdCache,
                statisticDataPointAdapterFactory);
    }


    @Override
    public EventStoreTimeIntervalEnum getInterval() {
        return timeInterval;
    }

    @Override
    public void addAggregatedEvents(final StatisticType statisticType,
                                    final Map<StatEventKey, StatAggregate> aggregatedEvents) {

//        LOGGER.ifDebugIsEnabled(() -> {
//            LOGGER.debug("putEventsMap key count: {}", putEventsMap.size());
//
//            aggregatedEvents.forEach((statKey, statAggregate) -> {
//                putEventsMap.computeIfPresent(statKey, (k, v) -> {
//                    LOGGER.debug("Existing key {}", k.toString());
//                    LOGGER.debug("New      key {}", statKey.toString());
//                    LOGGER.debug("Seen duplicate key");
////                    throw new RuntimeException(String.format("Key %s already exists with agg %s, new agg is %s", statKey, v, statAggregate));
//                    return v.aggregate(statAggregate, 100);
//                });
//                putEventsMap.put(statKey, statAggregate);
//            });
//        });

        //TODO if we introduce another stat type of STATE then the STATE and VALUE value objects used
        //for checking and setting should share an interface that has an aggregate method
        //and knows how to (de)serialize itself, thus this class doesn't have to care about the type of aggregate.
        //Could change StatAggregate to be an interface for this purpose with the EventID stuff changed to composition
        //instead.
        //Count stats will need to be handled differently though as they use a different approach, i.e. increments
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


        //keep a counter of the number of puts by stat type since this instance was last re-started
        //useful as an indication of how it is functioning.  Could put the count to a stat of its own
        //for long term tracking of performance
        putCounterMap.get(statisticType).add(aggregatedEvents.size());
    }

    private void putAggregatedEventsCount(final Map<StatEventKey, StatAggregate> aggregatedEvents) {

        LOGGER.trace(() -> String.format("putAggregatedEventsCount called with size %s", aggregatedEvents.size()));

        //Convert the aggregated events into row keys and cell qualifiers then
        //group them by rowkey so we have a list of cell increments for each row
        //meaning hbase can lock a row and make multiple changes to it at once.
        //Tuple2 is (RowKey, CountCellIncrementHolder)
        Map<RowKey, List<CountCellIncrementHolder>> rowData = aggregatedEvents.entrySet().stream()
                .map(entry -> {
                    CellQualifier cellQualifier = rowKeyBuilder.buildCellQualifier(entry.getKey());
                    long countIncrement = ((CountAggregate) entry.getValue()).getAggregatedCount();
                    return new Tuple2<>(
                            cellQualifier.getRowKey(),
                            new CountCellIncrementHolder(cellQualifier.getColumnQualifier(), countIncrement));
                })
                .collect(Collectors.groupingBy(
                        Tuple2::_1, Collectors.mapping(Tuple2::_2, Collectors.toList())));

        addMultipleCounts(rowData);
    }

    private void putAggregatedEventsValue(final Map<StatEventKey, StatAggregate> aggregatedEvents) {

        LOGGER.trace(() -> String.format("putAggregatedEventsValue called with size %s", aggregatedEvents.size()));
        //TODO ValueCellValue and ValueAggregate are essentially the same thing. Should probably keep Value Aggregate
        //and put any additional code from VCV into it.
        aggregatedEvents.forEach((statKey, statAggregate) -> {
            CellQualifier cellQualifier = rowKeyBuilder.buildCellQualifier(statKey);
            ValueAggregate valueAggregate;
            try {
                valueAggregate = (ValueAggregate) statAggregate;
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("StatAggregate %s is of the wrong type",
                        statAggregate.getClass().getName()));
            }
            ValueCellValue valueCellValue = new ValueCellValue(
                    valueAggregate.getCount(),
                    valueAggregate.getAggregatedValue(),
                    valueAggregate.getMinValue(),
                    valueAggregate.getMaxValue());

            addValue(cellQualifier, valueCellValue);
        });
    }

    @Override
    public long getCellsPutCount(final StatisticType statisticType) {
        //longaddr sum is not a concurrent snapshot but for this purpose is fine
        return putCounterMap.get(statisticType).sum();
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
        LOGGER.trace(() -> String.format("%s puts sent to HBase", actions.size()));

        // LOGGER.info("Sent %s ADDs to HBase from thread %s in %s ms",
        // cellQualifiersFromBuffer.size(),
        // Thread.currentThread().getName(), (System.currentTimeMillis() -
        // startTime));

    }

    /**
     * Builds a single {@link Increment} object for a row, with one-many cell
     * increments in that row
     *
     * @param rowKey The rowKey of the row to be updated
     * @param cells  A list of objects containing the column qualifier and cell
     *               increment value
     * @return The completed {@link Increment} object
     */
    private Increment createIncrementOperation(final RowKey rowKey, final List<CountCellIncrementHolder> cells) {
        LOGGER.trace(() -> String.format("createIncrementOperation called for rowKey: %s with cell count %s",
                rowKey.toString(),
                cells.size()));

        final Increment increment = new Increment(rowKey.asByteArray());

        // TODO HBase 2.0 has Increment.setReturnResults to allow you to prevent
        // the return of the new
        // value to improve performance. In our case we don't care about the new
        // value so when we
        // upgrade to HBase 2.0 we need to add this line in.
        // increment.setReturnResults(false);

        //if we have multiple CCIHs for the same rowKey/colQual then hbase seems to only process one of them
        //Due to the way the data is passed through to this method we should not get multiple increments for the
        //same rowKey/colQual so we will not check for it due to the cost of doing that.
        for (final CountCellIncrementHolder cell : cells) {
            increment.addColumn(EventStoreColumnFamily.COUNTS.asByteArray(), cell.getColumnQualifier().getBytes(),
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
                byte[] bColumnFamily = EventStoreColumnFamily.VALUES.asByteArray();
                ColumnQualifier columnQualifier = cellQualifier.getColumnQualifier();
                byte[] bColumnQualifier = columnQualifier.getBytes();
                byte[] bRowKey = cellQualifier.getRowKey().asByteArray();
                final Get get = new Get(bRowKey);
                get.addColumn(bColumnFamily, bColumnQualifier);

                final Result result = doGet(tableInterface, get);

                Cell existingCell = result.getColumnLatestCell(bColumnFamily, bColumnQualifier);

                final ValueCellValue currCellValue;
                final ValueCellValue newCellValue;

                if (existingCell != null) {
                    currCellValue = new ValueCellValue(existingCell.getValueArray(), existingCell.getValueOffset(), existingCell.getValueLength());

                    // aggregate the new value into the existing cell, incrementing
                    // the count and working out the max/min
                    newCellValue = currCellValue.addAggregatedValues(valueCellValue);
                    LOGGER.trace("Aggregating, currCellValue: {}, newCellValue: {}", currCellValue, newCellValue);
                } else {
                    //nothing there for this rowKey/colQual so just use our value as is
                    currCellValue = null;
                    newCellValue = valueCellValue;
                    LOGGER.trace("Cell is empty, newCellValue: {}", newCellValue);
                }

                // construct the put containing the new aggregated cell value
                final Put put = new Put(bRowKey).addColumn(
                        bColumnFamily,
                        bColumnQualifier,
                        newCellValue.asByteArray());

                // do a check and put - atomic operation to only do the put if
                // the cell value still looks like
                // currCellValue. If it fails go round again for another go
                hasPutSucceeded = doCheckAndPut(
                        tableInterface,
                        bRowKey,
                        bColumnFamily, bColumnQualifier,
                        (currCellValue == null ? null : currCellValue.asByteArray()),
                        put);

                if (hasPutSucceeded) {
                    // put worked so no need to retry
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
    public StatisticDataSet getStatisticsData(final UniqueIdCache uniqueIdCache,
                                              final StatisticConfiguration statisticConfiguration,
                                              final RollUpBitMask rollUpBitMask,
                                              final SearchStatisticsCriteria criteria) {

        LOGGER.debug(() -> String.format("getStatisticsData called, store: %s, statUuid: %s, statName: %s, type: %s, mask: %s, criteria: %s",
                timeInterval,
                statisticConfiguration.getUuid(),
                statisticConfiguration.getName(),
                statisticConfiguration.getStatisticType(),
                rollUpBitMask,
                criteria));

        final Period period = criteria.getPeriod();

        final StatisticType statisticType = statisticConfiguration.getStatisticType();

        LOGGER.debug(() -> String.format("Using time period: [%s] to [%s]",
                DateUtil.createNormalDateTimeString(period.getFrom()),
                DateUtil.createNormalDateTimeString(period.getTo())));

        String statUuid = statisticConfiguration.getUuid();

        final Scan scan = buildBasicScan(rollUpBitMask, period, statisticType, statUuid);

        // Query the event store over the given range.
        // We may only want part of the row from the first row and last row
        // but will want all columns from the rest of the rows. This is
        // because the start/end time may not land exactly on the row key
        // time interval boundaries.

        final Table tableInterface = getTable();

        addTagValueFilter(scan, criteria, uniqueIdCache);

        final ResultScanner scanner = getScanner(tableInterface, scan);

        // object to hold all the data returned
        final StatisticDataSet statisticDataSet = new StatisticDataSet(statUuid, statisticType);

        try {
            final long periodFrom = period.getFromOrElse(0L);
            final long periodTo = period.getToOrElse(Long.MAX_VALUE);

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
                // loop through each cell in the row each cell has a column qualifier that is the interval
                // number in the row interval e.g. if the row interval is hourly then the 5s value will
                // have a column qualifier of 5 and there will be up to 3600 columns
                while (cellScanner.advance()) {
                    final Cell cell = cellScanner.current();

                    ColumnQualifier columnQualifier = ColumnQualifier.from(cell.getQualifierArray(), cell.getQualifierOffset());

                    final CellQualifier cellQualifier = rowKeyBuilder.buildCellQualifier(rowKeyObject, columnQualifier);

                    final long fullTimestamp = cellQualifier.getFullTimestamp();

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("ColQualBytes: " + columnQualifier
                                + " ColQualInt: " + columnQualifier.getValue() + " FullTimestamp: "
                                + DateUtil.createNormalDateTimeString(fullTimestamp));
                    }

                    // filter the cell to ensure it is in the period we are after
                    //periodTo is exclusive
                    if (fullTimestamp >= periodFrom && fullTimestamp < periodTo) {

                        final StatisticDataPointAdapter adapter = statisticDataPointAdapterFactory.getAdapter(statisticType);
                        final StatisticDataPoint dataPoint = adapter.convertCell(
                                statUuid,
                                fullTimestamp,
                                timeInterval,
                                tags,
                                cell.getValueArray(),
                                cell.getValueOffset(),
                                cell.getValueLength());

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

    private Scan buildBasicScan(final RollUpBitMask rollUpBitMask,
                                final Period period,
                                final StatisticType statisticType,
                                final String statUuid) {

        final Scan scan = new Scan();

        final Optional<RowKey> optStartRowKey = Optional.ofNullable(period.getFrom())
                .map(fromMsInc ->
                        rowKeyBuilder.buildStartKey(statUuid, rollUpBitMask, fromMsInc));

        // we want to work from an inclusive time as we are working in row intervals
        final Optional<RowKey> optEndRowKeyExclusive = Optional.ofNullable(period.getToInclusive())
                .map(toMsInc ->
                        rowKeyBuilder.buildEndKey(statUuid, rollUpBitMask, toMsInc));

        if (LOGGER.isDebugEnabled()) {
            logStartStopKeys(optStartRowKey, optEndRowKeyExclusive);
        }

        if (period.isBounded()) {
            // From --> To
            optStartRowKey.map(RowKey::asByteArray).ifPresent(scan::setStartRow);
            optEndRowKeyExclusive.map(RowKey::asByteArray).ifPresent(scan::setStopRow);

        } else if (period.hasFrom() && !period.hasTo()) {
            // From ---->
            optStartRowKey.map(RowKey::asByteArray).ifPresent(scan::setStartRow);

            //need an exclusive end key but the simplest thing is to build an end key from the last possible
            //partial timestamp of the row key. While this isn't actually an exclusive stop key we
            //will never get close to the last partial timestamp in this system's lifetime.
            byte[] bStopKey = rowKeyBuilder.buildEndKeyBytes(statUuid, rollUpBitMask);
            scan.setStopRow(bStopKey);

        } else if (!period.hasFrom() && period.hasTo()) {
            // ----> To
            byte[] bStartKey = rowKeyBuilder.buildStartKeyBytes(statUuid, rollUpBitMask);
            scan.setStartRow(bStartKey);
            optEndRowKeyExclusive.map(RowKey::asByteArray).ifPresent(scan::setStopRow);
        } else {
            //No time bounds at all so set a row prefix
            scan.setRowPrefixFilter(rowKeyBuilder.buildStartKeyBytes(statUuid, rollUpBitMask));
        }

        scan.setMaxVersions(1); //we should only be storing 1 version but setting anyway
        //TODO make a prop in the prop service
        scan.setCaching(1_000);

        // determine which column family to use based on the kind of data we are trying to query
        if (statisticType.equals(StatisticType.COUNT)) {
            scan.addFamily(EventStoreColumnFamily.COUNTS.asByteArray());
        } else {
            scan.addFamily(EventStoreColumnFamily.VALUES.asByteArray());
        }
        return scan;
    }


    private void logStartStopKeys(final Optional<RowKey> optStartRowKey,
                                  final Optional<RowKey> optEndRowKeyExclusive) {
        LOGGER.debug("optStartRowKey: " + optStartRowKey
                .map(rowKeyBuilder::toPlainTextString)
                .orElse("Empty"));
        LOGGER.debug("optStartRowKey: " + optStartRowKey
                .map(RowKey::asByteArray)
                .map(ByteArrayUtils::byteArrayToHex)
                .orElse("Empty"));
        LOGGER.debug("endRowKey: " + optEndRowKeyExclusive
                .map(rowKeyBuilder::toPlainTextString)
                .orElse("Empty"));
        LOGGER.debug("endRowKey: " + optEndRowKeyExclusive
                .map(RowKey::asByteArray)
                .map(ByteArrayUtils::byteArrayToHex)
                .orElse("Empty"));
    }

    @Override
    public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                                      final StatisticConfiguration statisticConfiguration,
                                      final RollUpBitMask rollUpBitMask,
                                      final Period period) {
        boolean isFound = false;
        final String statUuid = statisticConfiguration.getUuid();

        final UID statUuidUid = uniqueIdCache.getUniqueIdOrDefault(statUuid);

        Scan scan = buildBasicScan(rollUpBitMask, period, statisticConfiguration.getStatisticType(), statUuid);

        // filter on rows with a key starting with the UID of our stat name
        final Filter prefixFilter = new PrefixFilter(statUuidUid.getUidBytes());
        // filter on the first row found
        final Filter pageFilter = new PageFilter(1);
        final Filter keyOnlyFilter = new KeyOnlyFilter();
        final FilterList filters = new FilterList(prefixFilter, pageFilter, keyOnlyFilter);
        scan.setFilter(filters);

        final Table tableInterface = getTable();
        final ResultScanner scanner = getScanner(tableInterface, scan);

        try {
            // the page filter may return more than one row as it is run on each
            // region so you may get one per region. We just want the first one we find
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
     * @param scan          The HBase scan object being used to scan the rows
     * @param criteria      The search criteria from the UI
     * @param uniqueIdCache The UID cache to convert strings into the UIDs understood by
     *                      HBase
     */
    private void addTagValueFilter(final Scan scan, final SearchStatisticsCriteria criteria, final UniqueIdCache uniqueIdCache) {
        // criteria may not have a filterTree on it
        if (!criteria.getFilterTermsTree().equals(FilterTermsTree.emptyTree())) {

            Filter tagValueFilter = new StatisticsTagValueFilter(
                    TagValueFilterTreeBuilder.buildTagValueFilterTree(criteria.getFilterTermsTree(), uniqueIdCache));

            scan.setFilter(tagValueFilter);
        }
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

        //TODO probably can use buildBasicScan() here
        final Scan scan = new Scan(startRowKey.asByteArray(), endRowKeyExclusive.asByteArray());
        scan.setMaxVersions(1);

        // TODO make this a global prop
        scan.setCaching(1_000);

        // add a keyOnlyFilter so we only get back the keys and not the data
        final FilterList filters = new FilterList(new KeyOnlyFilter());
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
                .orElseThrow(() -> new RuntimeException("No UID found for statistic " + statisticName))
                .getUidBytes();

        // add a keyOnlyFilter so we only get back the keys and not the data
        final FilterList filters = new FilterList(new KeyOnlyFilter());

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

}
