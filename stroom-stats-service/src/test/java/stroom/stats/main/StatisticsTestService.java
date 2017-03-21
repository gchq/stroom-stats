

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
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import javaslang.control.Try;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.ehcache.CacheManager;
import org.ehcache.UserManagedCache;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import stroom.query.api.DocRef;
import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionOperator.Op;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.ExpressionTerm.Condition;
import stroom.query.api.Query;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.cache.AbstractReadOnlyCacheLoaderWriter;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.EventStores;
import stroom.stats.hbase.HBaseStatisticsService;
import stroom.stats.hbase.RowKeyBuilder;
import stroom.stats.hbase.SimpleRowKeyBuilder;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.table.*;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.hbase.util.bytes.UnsignedBytes;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Script to create some base data for testing.
 */
public final class StatisticsTestService {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsTestService.class);
    private static final Random RANDOM = new Random();

    private CacheManager statCacheManager;
    private EventStoreTableFactory eventStoreTableFactory;
    private UniqueIdCache uniqueIdCache;
    private StatisticConfigurationService statisticConfigurationService;
    private EventStores eventStores;
    // MockStroomPropertyService propertyService = new MockStroomPropertyService();
    private StatisticsService statisticEventStore;
    private StroomPropertyService propertyService;
    private HBaseConnection hBaseConnection;
    private long id = 0;

    @Inject
    public StatisticsTestService(final CacheManager statCacheManager,
                                 final EventStoreTableFactory eventStoreTableFactory,
                                 final UniqueIdCache uniqueIdCache,
                                 final StatisticConfigurationService statisticConfigurationService,
                                 final EventStores eventStores,
                                 final StatisticsService statisticEventStore,
                                 final StroomPropertyService propertyService,
                                 final HBaseConnection hBaseConnection) {
        this.statCacheManager = statCacheManager;
        this.eventStoreTableFactory = eventStoreTableFactory;
        this.uniqueIdCache = uniqueIdCache;
        this.statisticConfigurationService = statisticConfigurationService;
        this.eventStores = eventStores;
        this.statisticEventStore = statisticEventStore;
        this.propertyService = propertyService;
        this.hBaseConnection = hBaseConnection;

    }

    public void run() throws IOException {
        setProperties();

        String eventName = "DesktopLogon";
        final Scan scan;
        final ResultScanner scanner;
        Writer writer;

        final long from = DateUtil.parseNormalDateTimeString("2015-01-01T00:00:00.000Z");
        final long to = DateUtil.parseNormalDateTimeString("2015-01-10T00:00:00.000Z");
        long rangeFrom;
        long rangeTo;

        final HBaseConnection tableConfiguration = getTableConfiguration();

        final EventStoreTimeIntervalEnum workingTimeInterval = EventStoreTimeIntervalEnum.HOUR;

        final HBaseTable hBaseTable = HBaseEventStoreTable.getInstance(workingTimeInterval, null, propertyService,
                hBaseConnection, uniqueIdCache);

        // HTableInterface table =
        // tableConfiguration.getTable(TableName.valueOf("hes"));
        // HTableInterface uidTable =
        // tableConfiguration.getTable(TableName.valueOf("uid"));

        // clearDownTable(hBaseTable.getTable());
        // clearDownTable(uidTable);
        clearDownAllTables(tableConfiguration);

        final RowKeyBuilder simpleRowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingTimeInterval);

        LOGGER.info("Putting data");
        // load random data into the event store tables
        for (int i = 0; i < 100_000; i++) {
            // Add a random number of seconds.
            // final long time = from + RANDOM.nextInt(86_400_000);
            final long time = from + (RANDOM.nextInt((int) ((to - from) / 1000)) * 1000);

            final StatisticTag userStatisticTag = new StatisticTag("user", createUser(5));
            final StatisticTag feedStatisticTag = new StatisticTag("feed", createFeed(5));
            final List<StatisticTag> statisticTags = new ArrayList<StatisticTag>();
            statisticTags.add(userStatisticTag);
            statisticTags.add(feedStatisticTag);

            final StatisticEvent event = new StatisticEvent(time, eventName, statisticTags, 1L);

            LOGGER.trace("Putting event: " + event.toString());

            statisticEventStore.putEvents(Collections.singletonList(event));
        }

        // get all the results back from the hourly event store to see what we
        // hold
        scanAllData(hBaseTable, simpleRowKeyBuilder, EventStoreColumnFamily.COUNTS);

        LOGGER.info("Scan event store for an event name and a time range");

        // now do a scan of a particular time range and filtering on particular
        // tags/values

        rangeFrom = DateUtil.parseNormalDateTimeString("2010-01-01T03:00:00.000Z");
        rangeTo = DateUtil.parseNormalDateTimeString("2010-01-01T09:00:00.000Z");

        writer = Files.newBufferedWriter(new File("chartData.csv").toPath(), UTF_8);

        StatisticDataSet statisticDataSet = performSearch(eventName, rangeFrom, rangeTo, null);

        // dumpStatisticsData(statisticDataSet);

        writer.close();

        LOGGER.info("Scan event store for an event name, a time range, and particular tags");

        // now do a scan of a particular time range and filtering on particular
        // tags/values

        rangeFrom = DateUtil.parseNormalDateTimeString("2010-01-01T03:00:00.000Z");
        rangeTo = DateUtil.parseNormalDateTimeString("2010-01-01T09:00:00.000Z");

        final ExpressionOperator opNodeOr = new ExpressionOperator(
                true,
                Op.OR,
                new ExpressionTerm("user", Condition.EQUALS, "user31"),
                new ExpressionTerm("feed", Condition.EQUALS, "TEST-EVENTS_3"));

        // if it hangs here then it is probably because you have not deployed
        // the TagValueFilter jar onto the HBase
        // nodes. See the blurb at the top of the TagValueFilter class.
        statisticDataSet = performSearch(eventName, rangeFrom, rangeTo, opNodeOr);

        // ########################################################################################################

        LOGGER.info("Load some value data and then scan all rows");

        eventName = "CPU usage";

        // load random data into the event store tables
        for (int i = 0; i < 10; i++) {
            // Add a random number of seconds.
            final long time = from + RANDOM.nextInt(10_000);

            final StatisticTag hostStatisticTag = new StatisticTag("host", createHost(2));
            final StatisticTag feedStatisticTag = new StatisticTag("feed", createFeed(1));
            final List<StatisticTag> statisticTags = new ArrayList<StatisticTag>();
            statisticTags.add(hostStatisticTag);
            statisticTags.add(feedStatisticTag);

            final double cpuPercent = RANDOM.nextDouble() * 100;

            final StatisticEvent event = new StatisticEvent(time, eventName, statisticTags, cpuPercent);

            LOGGER.trace("Putting event: " + event.toString());

            statisticEventStore.putEvents(Collections.singletonList(event));
        }

        scanAllData(hBaseTable, simpleRowKeyBuilder, EventStoreColumnFamily.VALUES);

        rangeFrom = DateUtil.parseNormalDateTimeString("2010-01-01T00:00:00.000Z");
        rangeTo = DateUtil.parseNormalDateTimeString("2010-01-01T01:00:00.000Z");

        statisticDataSet = performSearch(eventName, rangeFrom, rangeTo, null);

        ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting().flushAllEvents();

        LOGGER.info("All done!");
    }

    private StatisticDataSet performSearch(final String eventName, final long rangeFrom, final long rangeTo,
                                           final ExpressionItem additionalFilterBranch) {
        StatisticDataSet statisticDataSet;

        final StatisticConfiguration statisticConfiguration = statisticConfigurationService
                .fetchStatisticConfigurationByName(eventName)
                .orElseThrow(() -> new RuntimeException("StatisticConfiguration not found with name " + eventName));

        final DocRef docRef = new DocRef(statisticConfiguration.getType(), statisticConfiguration.getUuid());

        final ExpressionTerm dateTerm = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Condition.BETWEEN,
                rangeFrom + "," + rangeTo);

        List<ExpressionItem> children = new ArrayList<>();
        children.add(dateTerm);
        if (additionalFilterBranch != null) {
            children.add(additionalFilterBranch);
        }
        final ExpressionOperator root = new ExpressionOperator(
                true,
                Op.AND,
                children
        );


        final Query query = new Query(docRef, root, null);

        statisticDataSet = statisticEventStore.searchStatisticsData(query, statisticConfiguration);

        return statisticDataSet;
    }

    /**
     * This method relies on data being present in the hbase data base to search
     * on.
     */
    public void runRef() {
        LOGGER.info("Started");

        setProperties();

        final String statisticName = "ReadWriteVolumeBytes";
        long rangeFrom;
        long rangeTo;

        rangeFrom = DateUtil.parseNormalDateTimeString("2014-11-08T01:30:00.000Z");
        rangeTo = DateUtil.parseNormalDateTimeString("2014-11-11T23:00:00.000Z");

        final ExpressionTerm termNodeUser = new ExpressionTerm("userId", Condition.EQUALS, "someuser");

        // FindEventCriteria criteria = new FindEventCriteria(new
        // Period(rangeFrom, rangeTo), seriesList, filterTree,
        // AggregationMode.COUNT, new
        // BucketSize(BucketSize.PredefinedBucketSize.MINUTE));

        // FindEventCriteria criteria = new FindEventCriteria(new
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

    public void perfTest() throws IOException, InterruptedException {
        LOGGER.info("Started");

        setProperties();

        final HBaseConnection tableConfiguration = getTableConfiguration();
        clearDownAllTables(tableConfiguration);

        final List<StatisticEvent> eventList = new ArrayList<StatisticEvent>();

        final long from = DateUtil.parseNormalDateTimeString("2009-01-01T00:00:00.000Z");

        final BlockingQueue<StatisticEvent> eventQueue = new LinkedBlockingQueue<StatisticEvent>(100_000_000);

        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final ScheduledExecutorService debugSnapshotScheduler = Executors.newScheduledThreadPool(1);

        final UniqueIdCache uniqueIdCache = ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting()
                .getUniqueIdCache();

        final int itemsPutOnQueue = 10_000;

        final Runnable eventCreator = new Runnable() {
            @Override
            public void run() {
                int j = 0;
                for (int i = 0; i < itemsPutOnQueue; i++) {
                    // Add a random number of seconds.
                    // final long time = from +
                    // RANDOM.nextInt(Integer.MAX_VALUE);
                    final long time = from + RANDOM.nextInt(1000 * 60 * 60 * 4);

                    final StatisticTag userStatisticTag = new StatisticTag("user", createUser(3));
                    final StatisticTag feedStatisticTag = new StatisticTag("feed", createFeed(2));
                    final StatisticTag ipStatisticTag = new StatisticTag("ipAddress", createIP(2));
                    final List<StatisticTag> statisticTags = new ArrayList<StatisticTag>();
                    statisticTags.add(userStatisticTag);
                    statisticTags.add(feedStatisticTag);
                    statisticTags.add(ipStatisticTag);

                    final StatisticEvent event = new StatisticEvent(time, "TestStatName" + RANDOM.nextInt(3),
                            statisticTags, 1L);

                    // eventList.add(event);
                    eventQueue.add(event);

                    // pre load the UI cache with all the tag values
                    for (final StatisticTag statisticTag : statisticTags) {
                        uniqueIdCache.getOrCreateId(statisticTag.getValue());
                        uniqueIdCache.getOrCreateId(statisticTag.getTag());
                    }
                    if (++j % 100_000 == 0) {
                        LOGGER.info("Created {} event records, queue size: {} ", j, eventQueue.size());
                    }
                }
            }
        };

        // kick off the event creation
        executor.execute(eventCreator);

        // for (Event event : eventList) {
        // // LOGGER.trace("Event: {}", event);
        // }

        // pre load the event stores and table configuration
        try {
            // statisticEventStore.searchStatisticsData(null);
        } catch (final Exception e) {
            // do nothing
        }

        LOGGER.info("Starting puts");
        LOGGER.info(
                "-----------------------------------------------------------------------------------------------------");

        Thread.sleep(4000);
        LOGGER.info("done sleep");

        final long startTime = System.currentTimeMillis();

        // doTest(eventList, executor);

        final Runnable snaphotTaker = new Runnable() {
            @Override
            public void run() {
                if (eventQueue != null && eventQueue.size() > 0) {
                    LOGGER.debug(() -> String.format("Taking snapshot of event queue (size: %s):", eventQueue.size()));
                }
            }
        };

        final ScheduledFuture<?> snaphotTakerFuture = debugSnapshotScheduler.scheduleAtFixedRate(snaphotTaker, 10, 30,
                TimeUnit.SECONDS);

        int j = 0;
        while (true) {
            takeEvent(eventQueue, executor);
            if (++j % 100_000 == 0) {
                LOGGER.info("Given {} tasks to the executorService, queue size: {}", j, eventQueue.size());
            }
            if (j == itemsPutOnQueue) {
                break;
            }
        }

        // executor.shutdown();

        // executor.awaitTermination(10, TimeUnit.MINUTES);

        LOGGER.info("threads all done");

        // ((StatisticsServiceImpl)
        // statisticsService).getEventStoresForTesting().flushAllEvents();

        final long runTimeSecs = (System.currentTimeMillis() - startTime);

        LOGGER.info("All done! Runtime: " + runTimeSecs + "ms");
        countAllTables(tableConfiguration);
        // Thread.sleep(500000);

    }

    private void doTest(final List<StatisticEvent> eventList, final ExecutorService executor)
            throws InterruptedException {
        for (final StatisticEvent statisticEvent : eventList) {
            final Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    // LOGGER.info("Starting thread: " +
                    // Thread.currentThread().getName());
                    statisticEventStore.putEvents(Collections.singletonList(statisticEvent));
                }
            };

            executor.execute(runnable);
        }

        executor.shutdown();

        executor.awaitTermination(10, TimeUnit.MINUTES);
    }

    private void putEvent(final StatisticEvent event, final ExecutorService executor) throws InterruptedException {
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                // LOGGER.info("Starting thread: " +
                // Thread.currentThread().getName());
                statisticEventStore.putEvents(Collections.singletonList(event));
            }
        };

        executor.execute(runnable);

    }

    private void takeEvent(final BlockingQueue<StatisticEvent> queue, final ExecutorService executor)
            throws InterruptedException {
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                // LOGGER.info("Starting thread: " +
                // Thread.currentThread().getName());
                try {
                    statisticEventStore.putEvents(Collections.singletonList(queue.take()));
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        executor.execute(runnable);

    }

    public void cacheTest() throws IOException, InterruptedException {
        LOGGER.info("Started");

        final HBaseConnection tableConfiguration = getTableConfiguration();
        clearDownAllTables(tableConfiguration);

        final List<StatisticEvent> eventList = new ArrayList<StatisticEvent>();

        final long from = DateUtil.parseNormalDateTimeString("2009-01-01T00:00:00.000Z");

        final Queue<StatisticEvent> eventQueue = new LinkedBlockingQueue<StatisticEvent>();

        final List<StatisticTag> allStatisticTags = new ArrayList<StatisticTag>();

        LOGGER.info("Starting event building");

        for (int i = 0; i < 30_000; i++) {
            // Add a random number of seconds.
            // final long time = from + RANDOM.nextInt(Integer.MAX_VALUE);
            final long time = from + RANDOM.nextInt(1000 * 60 * 60 * 24 * 2);

            final StatisticTag userStatisticTag = new StatisticTag("user", createUser(300));
            final StatisticTag feedStatisticTag = new StatisticTag("feed", createFeed(100));
            final StatisticTag ipStatisticTag = new StatisticTag("ipAddress", createIP(100));
            final List<StatisticTag> tagValues = new ArrayList<StatisticTag>();
            tagValues.add(userStatisticTag);
            tagValues.add(feedStatisticTag);
            tagValues.add(ipStatisticTag);

            final StatisticEvent event = new StatisticEvent(time, "TestStatName", tagValues, 1L);

            eventList.add(event);
            // eventQueue.add(event);

            allStatisticTags.addAll(tagValues);

        }

        LOGGER.info("allTagValues size:" + allStatisticTags.size());

        // Thread.sleep(15000);

        long startTime = System.currentTimeMillis();

        LOGGER.info("Starting preloading");
        // pre load the UI cache with all the tag values
        for (final StatisticTag statisticTag : allStatisticTags) {
            ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting().getUniqueIdCache()
                    .getOrCreateId(statisticTag.getValue());
            ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting().getUniqueIdCache()
                    .getOrCreateId(statisticTag.getTag());
        }

        LOGGER.info("Preloading finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("EHCache size:" + ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting()
                .getUniqueIdCache().getCacheSize());

        // Thread.sleep(40_000);

        LOGGER.info("Starting cache test");

        final UniqueIdCache uniqueIdCache = ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting()
                .getUniqueIdCache();

        startTime = System.currentTimeMillis();

        for (final StatisticTag statisticTag : allStatisticTags) {
            uniqueIdCache.getOrCreateId(statisticTag.getValue());
            uniqueIdCache.getOrCreateId(statisticTag.getTag());
        }

        LOGGER.info("Cache test finished in " + (System.currentTimeMillis() - startTime) + "ms");

        // Thread.sleep(2_000_000);

        LOGGER.info("EHCache size:" + ((HBaseStatisticsService) statisticEventStore).getEventStoresForTesting()
                .getUniqueIdCache().getCacheSize());

        LOGGER.info("Load into hashmap");

        startTime = System.currentTimeMillis();

        final Map<String, byte[]> hashMapCache = new HashMap<>();

        final int width = 3;

        for (final StatisticTag statisticTag : allStatisticTags) {
            byte[] bNewId = new byte[width];
            UnsignedBytes.put(bNewId, 0, width, nextId());

            hashMapCache.put(statisticTag.getTag(), bNewId);

            bNewId = new byte[width];
            UnsignedBytes.put(bNewId, 0, width, nextId());

            hashMapCache.put(statisticTag.getValue(), bNewId);
        }

        LOGGER.info("Hashmap load finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("hashMapCache size:" + hashMapCache.size());

        LOGGER.info("Get from hashmap");

        startTime = System.currentTimeMillis();

        for (final StatisticTag statisticTag : allStatisticTags) {
            hashMapCache.get(statisticTag.getTag());
            hashMapCache.get(statisticTag.getValue());
        }

        LOGGER.info("Hashmap get finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("Put to new EHCache");

        startTime = System.currentTimeMillis();

        UserManagedCache<String, byte[]> cache = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, byte[].class)
                .withResourcePools(ResourcePoolsBuilder.heap(1000000))
                .withExpiry(Expirations.timeToLiveExpiration(Duration.of(600, TimeUnit.SECONDS)))
                .withExpiry(Expirations.timeToIdleExpiration(Duration.of(3600, TimeUnit.SECONDS)))
                .build(true);


        for (final StatisticTag statisticTag : allStatisticTags) {
            byte[] bNewId = new byte[width];
            UnsignedBytes.put(bNewId, 0, width, nextId());

            cache.put(statisticTag.getTag(), bNewId);

            bNewId = new byte[width];
            UnsignedBytes.put(bNewId, 0, width, nextId());

            cache.put(statisticTag.getValue(), bNewId);
        }

        LOGGER.info("New EHCache Put finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("hashMapCache size:" + cache.getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize());

        LOGGER.info("Get from new EHCache");

        startTime = System.currentTimeMillis();

        for (final StatisticTag statisticTag : allStatisticTags) {
            cache.get(statisticTag.getTag());

            cache.get(statisticTag.getValue());
        }

        LOGGER.info("New EHCache get finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("Put to new selfPopEHCache");

        startTime = System.currentTimeMillis();

        CacheLoaderWriter<String, byte[]> loaderWriter = new AbstractReadOnlyCacheLoaderWriter<String, byte[]>() {
            @Override
            public byte[] load(final String key) throws Exception {
                final byte[] bNewId = new byte[width];
                UnsignedBytes.put(bNewId, 0, width, nextId());
                return bNewId;
            }

            @Override
            public Map<String, byte[]> loadAll(final Iterable<? extends String> keys) throws BulkCacheLoadingException, Exception {
                throw new UnsupportedOperationException(String.format("Unsupported"));
            }
        };

        UserManagedCache<String, byte[]> selfPopCache = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, byte[].class)
                .withResourcePools(ResourcePoolsBuilder.heap(1000000))
                .withExpiry(Expirations.timeToLiveExpiration(Duration.of(600, TimeUnit.SECONDS)))
                .withExpiry(Expirations.timeToIdleExpiration(Duration.of(3600, TimeUnit.SECONDS)))
                .withLoaderWriter(loaderWriter)
                .build(true);


        for (final StatisticTag statisticTag : allStatisticTags) {
            selfPopCache.get(statisticTag.getTag());

            selfPopCache.get(statisticTag.getValue());
        }

        LOGGER.info("New selfPopEHCache Put finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("selfPopEHCache size:" + selfPopCache.getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize());

        LOGGER.info("Get from new selfPopEHCache");

        startTime = System.currentTimeMillis();

        for (final StatisticTag statisticTag : allStatisticTags) {
            final byte[] t = selfPopCache.get(statisticTag.getTag());
            final byte[] v = selfPopCache.get(statisticTag.getValue());
        }

        LOGGER.info("New selfPopEHCache get finished in " + (System.currentTimeMillis() - startTime) + "ms");

        LOGGER.info("Done");

        // Thread.sleep(300000);

    }

    private long nextId() {
        return this.id++;
    }

    private void dumpStatisticsData(final StatisticDataSet statisticDataSet) {
        LOGGER.info("Dumping data for statistic: " + statisticDataSet.getStatisticName());

        final List<String> records = new ArrayList<>();

        for (final StatisticDataPoint dataPoint : statisticDataSet) {
            records.add("  " + DateUtil.createNormalDateTimeString(dataPoint.getTimeMs()) + " - " + dataPoint.getCount()
                    + " - " + dataPoint.getValue());
        }

        Collections.sort(records);

        for (final String str : records) {
            LOGGER.info(str);
        }
    }

    private void scanRow(final Result result, final RowKeyBuilder simpleRowKeyBuilder, final RowKey rowKey,
                         final StatisticType statsType) throws IOException {
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
            final long columnIntervalSize = EventStoreTimeIntervalEnum.SECOND.columnInterval();
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

    private void scanAllData(final HBaseTable hBaseTable, final RowKeyBuilder simpleRowKeyBuilder,
                             final EventStoreColumnFamily eventStoreColumnFamily) throws IOException {
        scanAllData(hBaseTable, simpleRowKeyBuilder, eventStoreColumnFamily, Integer.MAX_VALUE);

    }

    private void scanAllData(final HBaseTable hBaseTable, final RowKeyBuilder simpleRowKeyBuilder,
                             final EventStoreColumnFamily eventStoreColumnFamily, final int rowLimit) throws IOException {
        // get all rows from the counts column family, latest version only
        final Scan scan = new Scan().setMaxVersions(1).addFamily(eventStoreColumnFamily.asByteArray());

        int rowCount = 0;

        // get all the results back from the hourly event store to see what we
        // hold
        final Table tableInterface = hBaseTable.getTable();
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

            // scanRow(result, simpleRowKeyBuilder, rowKeyObject, statsType);

            rowCount++;

        }
        scanner.close();
        HBaseTable.closeTable(tableInterface);
    }

    public void scanAllData(final EventStoreTimeIntervalEnum timeInterval,
                            final EventStoreColumnFamily eventStoreColumnFamily,
                            final int rowLimit) throws IOException {

        LOGGER.info("Scanning all {} data in event store {}", eventStoreColumnFamily, timeInterval.getDisplayValue());

        HBaseTable hBaseTable = (HBaseTable) eventStoreTableFactory.getEventStoreTable(timeInterval);
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
            hBaseTable = HBaseEventStoreTable.getInstance(interval, null, propertyService, hBaseConnection, uniqueIdCache);
            final Table tableInterface = hBaseTable.getTable();
            clearDownTable(tableInterface);
            tableInterface.close();
        }

    }

    private void countAllTables(final HBaseConnection tableConfiguration) throws IOException {
        HBaseTable hBaseTable;
        for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
            hBaseTable = HBaseEventStoreTable.getInstance(interval, null, propertyService, hBaseConnection, uniqueIdCache);
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

    private void setProperties() {
        //
        // propertyService.setGlobalProperty(CommonStatisticConstants.STROOM_STATISTIC_ENGINES_PROPERTY_NAME,
        // "hbase");
        //
        // propertyService.setGlobalProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME,
        // Integer.toString(500));
        //
        // propertyService
        // .setGlobalProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME,
        // Long.toString(1000 * 60 * 2));
        //
        // propertyService.setGlobalProperty(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME,
        // Integer.toString(4));
        //
        // propertyService.setGlobalProperty(HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME,
        // Integer.toString(2_000));
        // propertyService.setGlobalProperty(HBaseStatisticConstants.NODE_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME,
        // Long.toString(1000 * 60 *
        // 2));
        //
        // propertyService.setGlobalProperty(HBaseStatisticConstants.DATA_STORE_PUT_BUFFER_MAX_SIZE_PROPERTY_NAME,
        // Integer.toString(1_000_000));
        // propertyService.setGlobalProperty(HBaseStatisticConstants.DATA_STORE_PUT_BUFFER_TAKE_COUNT_PROPERTY_NAME,
        // Integer.toString(100));
    }

    public static void main(final String[] args) throws Exception {

        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(StatisticsTestService.class);
            }
        });

        final StatisticsTestService statisticsTestServiceBean = injector.getInstance(StatisticsTestService.class);

        statisticsTestServiceBean.run();
    }
}
