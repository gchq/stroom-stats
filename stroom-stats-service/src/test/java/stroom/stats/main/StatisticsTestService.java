

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

package stroom.stats.main;

import com.google.common.collect.Iterables;
import javaslang.control.Try;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.query.api.DocRef;
import stroom.query.api.Query;
import stroom.query.api.SearchRequest;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.Period;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.EventStores;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.table.EventStoreColumnFamily;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.table.HBaseEventStoreTable;
import stroom.stats.hbase.table.HBaseTable;
import stroom.stats.hbase.table.HBaseUniqueIdForwardMapTable;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Script to create some base data for testing.
 */
public final class StatisticsTestService {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsTestService.class);
    private static final Random RANDOM = new Random();

    private EventStoreTableFactory eventStoreTableFactory;
    private UniqueIdCache uniqueIdCache;
    private StatisticConfigurationService statisticConfigurationService;
    private EventStores eventStores;
    // MockStroomPropertyService propertyService = new MockStroomPropertyService();
    private StatisticsService statisticsService;
    private StroomPropertyService propertyService;
    private HBaseConnection hBaseConnection;
    private long id = 0;

    @Inject
    public StatisticsTestService(final EventStoreTableFactory eventStoreTableFactory,
                                 final UniqueIdCache uniqueIdCache,
                                 final StatisticConfigurationService statisticConfigurationService,
                                 final EventStores eventStores,
                                 final StatisticsService statisticsService,
                                 final StroomPropertyService propertyService,
                                 final HBaseConnection hBaseConnection) {
        this.eventStoreTableFactory = eventStoreTableFactory;
        this.uniqueIdCache = uniqueIdCache;
        this.statisticConfigurationService = statisticConfigurationService;
        this.eventStores = eventStores;
        this.statisticsService = statisticsService;
        this.propertyService = propertyService;
        this.hBaseConnection = hBaseConnection;

    }


    private StatisticDataSet performSearch(final String eventName, final long rangeFrom, final long rangeTo,
                                           final FilterTermsTree.Node additionalFilterBranch) {
        StatisticDataSet statisticDataSet;

        final StatisticConfiguration statisticConfiguration = statisticConfigurationService
                .fetchStatisticConfigurationByName(eventName)
                .orElseThrow(() -> new RuntimeException("StatisticConfiguration not found with name " + eventName));

        final DocRef docRef = new DocRef(statisticConfiguration.getType(), statisticConfiguration.getUuid());


        List<FilterTermsTree.Node> children = new ArrayList<>();
        if (additionalFilterBranch != null) {
            children.add(additionalFilterBranch);
        }
        final FilterTermsTree.Node root = new FilterTermsTree.OperatorNode(
                FilterTermsTree.Operator.AND,
                children
        );


        SearchStatisticsCriteria searchStatisticsCriteria = SearchStatisticsCriteria
                .builder(new Period(rangeFrom, rangeTo), statisticConfiguration.getName())
                .setFilterTermsTree(new FilterTermsTree(root))
                .setRequiredDynamicFields(statisticConfiguration.getFieldNames())
                .build();

        statisticDataSet = statisticsService.searchStatisticsData(searchStatisticsCriteria, statisticConfiguration);

        return statisticDataSet;
    }

    /**
     * This method relies on data being present in the hbase data base to search
     * on.
     */
    public void runRef() {
        LOGGER.info("Started");

        final String statisticName = "ReadWriteVolumeBytes";
        long rangeFrom;
        long rangeTo;

        rangeFrom = DateUtil.parseNormalDateTimeString("2014-11-08T01:30:00.000Z");
        rangeTo = DateUtil.parseNormalDateTimeString("2014-11-11T23:00:00.000Z");

        final FilterTermsTree.TermNode termNodeUser = new FilterTermsTree.TermNode("userId", "someuser");

        // SearchStatisticsCriteria criteria = new SearchStatisticsCriteria(new
        // Period(rangeFrom, rangeTo), seriesList, filterTree,
        // AggregationMode.COUNT, new
        // BucketSize(BucketSize.PredefinedBucketSize.MINUTE));

        // SearchStatisticsCriteria criteria = new SearchStatisticsCriteria(new
        // Period(rangeFrom, rangeTo), seriesList,
        // AggregationMode.COUNT, new
        // BucketSize(BucketSize.PredefinedBucketSize.SECOND));

        // if it hangs here then it is probably because you have not deployed
        // the TagValueFilter jar onto the HBase
        // nodes. See the blurb at the top of the TagValueFilter class.
        LOGGER.info("Starting query");
        final StatisticDataSet statisticDataSet = performSearch(statisticName, rangeFrom, rangeTo, termNodeUser);

        dumpStatisticsData(statisticDataSet);

        LOGGER.info("All done!");
    }





    private long nextId() {
        return this.id++;
    }

    private void dumpStatisticsData(final StatisticDataSet statisticDataSet) {
        LOGGER.info("Dumping data for statistic: " + statisticDataSet.getStatisticName());

        final List<String> records = new ArrayList<>();

        for (final StatisticDataPoint dataPoint : statisticDataSet) {
            records.add("  " + DateUtil.createNormalDateTimeString(dataPoint.getTimeMs()) + " - " + dataPoint.getFieldValue(StatisticConfiguration.FIELD_NAME_COUNT)
                    + " - " + dataPoint.getFieldValue(StatisticConfiguration.FIELD_NAME_VALUE));
        }

        Collections.sort(records);

        for (final String str : records) {
            LOGGER.info(str);
        }
    }

    private void scanRow(final Result result, final RowKeyBuilder simpleRowKeyBuilder, final RowKey rowKey,
                         final StatisticType statsType, EventStoreTimeIntervalEnum interval) throws IOException {
        final CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            final Cell cell = cellScanner.current();

            // get the column qualifier
            final byte[] bTimeQualifier = new byte[cell.getQualifierLength()];
            System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), bTimeQualifier, 0,
                    cell.getQualifierLength());

            // convert this into a true time, albeit rounded to the column
            // interval granularity
            final long columnIntervalNo = Bytes.toInt(bTimeQualifier);
            final long columnIntervalSize = interval.columnInterval();
            final long columnTimeComponentMillis = columnIntervalNo * columnIntervalSize;
            final long rowKeyPartialTimeMillis = simpleRowKeyBuilder.getPartialTimestamp(rowKey);
            final long fullTimestamp = rowKeyPartialTimeMillis + columnTimeComponentMillis;

            LOGGER.debug("Col: [" + ByteArrayUtils.byteArrayToHex(bTimeQualifier) + "] - ["
                    + Bytes.toInt(bTimeQualifier) + "] - [" + fullTimestamp + "] - ["
                    + DateUtil.createNormalDateTimeString(fullTimestamp) + "]");

            final byte[] bValue = new byte[cell.getValueLength()];
            System.arraycopy(cell.getValueArray(), cell.getValueOffset(), bValue, 0, cell.getValueLength());

            switch (statsType) {
                case VALUE:
                    final ValueCellValue cellValue = new ValueCellValue(bValue);

                    LOGGER.debug("Val: " + cellValue);
                    break;
                case COUNT:
                    LOGGER.debug("Val: " + Bytes.toLong(bValue));
                    break;
            }

        }
    }

    private void scanAllData(final HBaseEventStoreTable hBaseEventStoreTable, final RowKeyBuilder simpleRowKeyBuilder,
                             final EventStoreColumnFamily eventStoreColumnFamily) throws IOException {
        scanAllData(hBaseEventStoreTable, simpleRowKeyBuilder, eventStoreColumnFamily, Integer.MAX_VALUE);

    }

    private void scanAllData(final HBaseEventStoreTable hbaseEventStoreTable, final RowKeyBuilder simpleRowKeyBuilder,
                             final EventStoreColumnFamily eventStoreColumnFamily, final int rowLimit) throws IOException {
        // get all rows from the passed column family, latest version only
        final Scan scan = new Scan().setMaxVersions(1).addFamily(eventStoreColumnFamily.asByteArray());

        int rowCount = 0;

        // get all the results back from the event store to see what we hold
        final Table tableInterface = hbaseEventStoreTable.getTable();
        final ResultScanner scanner = tableInterface.getScanner(scan);
        for (final Result result : scanner) {
            if (rowCount > rowLimit) {
                break;
            }

            final byte[] rowKey = result.getRow();

            // LOGGER.info(dumpByteArray(rowKey));

            // attempt to build an object from the raw row key bytes
            final RowKey rowKeyObject = new RowKey(rowKey);

            // LOGGER.info(rowKeyObject.toString());

            LOGGER.debug(simpleRowKeyBuilder.toPlainTextString(rowKeyObject));
            // LOGGER.trace(ByteArrayUtils.byteArrayToString(rowKey));
            LOGGER.trace(ByteArrayUtils.byteArrayToHex(rowKey));
            // LOGGER.trace(Bytes.toHex(rowKey));

            // now output the contents of the row
            StatisticType statsType;
            if (eventStoreColumnFamily.equals(EventStoreColumnFamily.COUNTS))
                statsType = StatisticType.COUNT;
            else
                statsType = StatisticType.VALUE;

            scanRow(result, simpleRowKeyBuilder, rowKeyObject, statsType, hbaseEventStoreTable.getInterval());

            rowCount++;

        }
        scanner.close();
        HBaseTable.closeTable(tableInterface);
    }

    public void scanAllData(final EventStoreTimeIntervalEnum timeInterval,
                            final EventStoreColumnFamily eventStoreColumnFamily,
                            final int rowLimit) throws IOException {

        LOGGER.info("Scanning all {} data in event store {}", eventStoreColumnFamily, timeInterval.getDisplayValue());

        HBaseEventStoreTable hBaseTable = (HBaseEventStoreTable) eventStoreTableFactory.getEventStoreTable(timeInterval);
//        final HBaseTable hBaseTable = HBaseEventStoreTable.getInstance(timeInterval, null, propertyService,
//                hBaseConnection, uniqueIdCache);

        final RowKeyBuilder simpleRowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, timeInterval);

        scanAllData(hBaseTable, simpleRowKeyBuilder, eventStoreColumnFamily, rowLimit);
    }

    public void purgeAllRowkeys() {
        System.out.println(eventStores);
    }

    public void scanUIDTable() throws IOException {
        // TableConfiguration tableConfiguration = getTableConfiguration();
        final HBaseUniqueIdForwardMapTable uidTable = new HBaseUniqueIdForwardMapTable(hBaseConnection);

        // UniqueIdCache uniqueIdCache = getUinqueIdCache(tableConfiguration);

        final Scan scan = new Scan().setMaxVersions(1).addFamily(Bytes.toBytes("i"));

        final Table tableInterface = uidTable.getTable();
        final ResultScanner scanner = tableInterface.getScanner(scan);

        final Writer writerU = Files.newBufferedWriter(new File("UID_U.csv").toPath(), UTF_8);
        final Writer writerV = Files.newBufferedWriter(new File("UID_V.csv").toPath(), UTF_8);

        String line = "";

        LOGGER.info("Dumping contents of UID table");

        for (final Result result : scanner) {
            final byte[] rowKey = result.getRow();
            String colQual;
            String type = "";
            byte[] valueColValue = null;

            final CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                final Cell cell = cellScanner.current();

                // get the column qualifier
                final byte[] bcolQual = new byte[cell.getQualifierLength()];
                System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), bcolQual, 0,
                        cell.getQualifierLength());

                colQual = Bytes.toString(bcolQual);

                final byte[] bCellVal = new byte[cell.getValueLength()];
                System.arraycopy(cell.getValueArray(), cell.getValueOffset(), bCellVal, 0, cell.getValueLength());

                if (colQual.equals("t")) {
                    // type column
                    type = Bytes.toString(bCellVal);

                } else if (colQual.equals("v")) {
                    // value column
                    valueColValue = bCellVal;
                }
            }

            if (type.equals("U")) {
                // row key is a UID o convert that to hex and convert the value
                // col value to a string

                line = type + "," + ByteArrayUtils.byteArrayToHex(rowKey) + "," + Bytes.toString(valueColValue);

                writerU.write(line + "\n");

            } else {
                line = type + "," + Bytes.toString(rowKey) + "," + ByteArrayUtils.byteArrayToHex(valueColValue);

                writerV.write(line + "\n");
            }

        }

        scanner.close();
        HBaseTable.closeTable(tableInterface);

        writerU.close();
        writerV.close();

    }

    public void testUIDTypeCache() throws IOException {
        final HBaseConnection tableConfiguration = getTableConfiguration();

        final String statName = "FeedStatus";

        final Try<UID> uniqueId = uniqueIdCache.getUniqueId(statName);

        LOGGER.info(uniqueId.getOrElse(UID.NOT_FOUND_UID).toAllForms());

        final String name = uniqueIdCache.getName(uniqueId.get());

        LOGGER.info(name);

        if (!statName.equals(name)) {
            throw new RuntimeException("Names don't match, name is: " + name);
        }
        LOGGER.info(name);

    }

    private void deleteAllRows(final Table table) throws IOException {
        final Scan scan = new Scan();
        final List<Delete> deleteList = new ArrayList<>();
        final ResultScanner results = table.getScanner(scan);
        for (final Result result : results) {
            deleteList.add(new Delete(result.getRow()));
        }
        results.close();
        table.delete(deleteList);
    }

    private int countRows(final Table table) throws IOException {
        int count = 0;
        final Scan scan = new Scan();
        scan.addFamily(EventStoreColumnFamily.COUNTS.asByteArray());
        scan.addFamily(EventStoreColumnFamily.VALUES.asByteArray());
        try (final ResultScanner results = table.getScanner(scan)) {
            count = Iterables.size(results);
        }
        return count;
    }

    private void clearDownAllTables(final HBaseConnection tableConfiguration) throws IOException {
        HBaseTable hBaseTable;
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            hBaseTable = (HBaseTable) eventStoreTableFactory.getEventStoreTable(interval);
            final Table tableInterface = hBaseTable.getTable();
            clearDownTable(tableInterface);
            tableInterface.close();
        }

    }

    private void countAllTables(final HBaseConnection tableConfiguration) throws IOException {
        HBaseTable hBaseTable;
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            hBaseTable = (HBaseTable) eventStoreTableFactory.getEventStoreTable(interval);
            final Table tableInterface = hBaseTable.getTable();
            LOGGER.info("Row count for " + hBaseTable.getName() + " (" + countRows(tableInterface) + ")");
            tableInterface.close();
        }
    }

    private void clearDownTable(final Table table) throws IOException {
        int count = countRows(table);
        LOGGER.info("Deleting all rows for " + table.getName() + " (" + count + ")");
        deleteAllRows(table);
        count = countRows(table);
        LOGGER.info("Rows remaining: " + count);
    }

    private String dumpByteArray(final byte[] arr) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : arr) {
            sb.append(b);
            sb.append(" ");
        }
        return sb.toString().replaceAll(" $", "");
    }

    private String createUser(final int n) {
        return "user" + (RANDOM.nextInt(n) + 1);
    }

    private String createHost(final int n) {
        return "HOST_" + (RANDOM.nextInt(n) + 1);
    }

    private String createFeed(final int n) {
        return "TEST-EVENTS_" + (RANDOM.nextInt(n));
    }

    private String createIP(final int n) {
        return "192.168.1." + (RANDOM.nextInt(n) + 1);
    }

    private String createIP() {
        return "192.168." + (RANDOM.nextInt(254) + 1) + "." + (RANDOM.nextInt(254) + 1);
    }

    private HBaseConnection getTableConfiguration() {
        return new HBaseConnection(propertyService);
    }


    private SearchRequest wrapQuery(Query query) {
        return new SearchRequest(null, query, Collections.emptyList(), ZoneOffset.UTC.getId(), false);
    }
}
