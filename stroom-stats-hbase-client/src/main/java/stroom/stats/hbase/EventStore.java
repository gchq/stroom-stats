

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

package stroom.stats.hbase;

import org.hibernate.cache.CacheException;
import stroom.stats.api.StatisticType;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.Period;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.table.EventStoreTable;
import stroom.stats.hbase.table.TableFactory;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class EventStore {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(EventStore.class);
    private final EventStoreTable eventStoreTable;
    private final EventStoreTimeIntervalEnum timeInterval;
    private final RowKeyBuilder rowKeyBuilder;
    private final StroomPropertyService propertyService;

    private final String purgeRetentionPeriodsPropertyKey;

    private int purgeIntervalsPropertyVal = -1;
    private final String purgeIntervalsPropertyValStr = null;

    /**
     * Create an event store to store events with the event time rounded to a
     * specified number of milliseconds.
     * <p>
     * There will be one instance of this class per time interval
     *
     * @param interval A TimeIntervalEnum instance describing what interval the store
     *                 should be configured with
     */
    public EventStore(final UniqueIdCache uidCache, final EventStoreTimeIntervalEnum interval,
                      final TableFactory tableFactory, final StroomPropertyService propertyService) {
        LOGGER.info("Initialising EventSore for interval {}", interval);

        this.eventStoreTable = tableFactory.getEventStoreTable(interval);
        this.timeInterval = interval;
        this.propertyService = propertyService;
        this.purgeRetentionPeriodsPropertyKey = HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                + interval.name().toLowerCase();


        //TODO do we want to cache the conversion of AggregatedEvents to CellQualifiers?
        rowKeyBuilder = new SimpleRowKeyBuilder(uidCache, interval);
//        rowKeyBuilder = CachedRowKeyBuilder.wrap(new SimpleRowKeyBuilder(uidCache, interval), rowKeyCache));
    }

    /**
     * Puts a batch of aggregated events into the store
     */
    public void putAggregatedEvents(final StatisticType statisticType, final List<AggregatedEvent> aggregatedEvents) {

        eventStoreTable.addAggregatedEvents(statisticType, aggregatedEvents);
    }


    public void flushAllEvents() {
        LOGGER.debug("flushAllEvents called for store: {}", this.timeInterval);
        eventStoreTable.shutdown();
    }

//    public int getPutBufferCount(final boolean isDeepCount) {
//        return eventStoreTable.getPutBufferCount(isDeepCount);
//    }
//
//    public int getAvailableBatchPutTaskPermits() {
//        return eventStoreTable.getAvailableBatchPutTaskPermits();
//    }

    public long getCellsPutCount(final StatisticType statisticType) {
        return eventStoreTable.getCellsPutCount(statisticType);
    }

    public StatisticDataSet getStatisticsData(final UniqueIdCache uniqueIdCache,
                                              final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask,
                                              final FindEventCriteria criteria) {
        return eventStoreTable.getStatisticsData(uniqueIdCache, statisticConfiguration, rollUpBitMask, criteria);

    }

    public EventStoreTimeIntervalEnum getTimeInterval() {
        return timeInterval;
    }

    public void shutdown() {
        eventStoreTable.shutdown();
    }

    /**
     * Looks in the store to see if the statistic name exists anywhere in the
     * table
     *
     * @param uniqueIdCache The UID cache to use to resolve UIDs
     * @return true if it exists
     */
    public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                                      final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask, final Period period) {
        // work out the limit of the retained data, assuming purge has just run
        final long purgeUpToTimeMs = calculatePurgeUpToTimeMs(System.currentTimeMillis());

        if (period.getFrom() < purgeUpToTimeMs) {
            // beginning of the search period is outside our retained data so
            // this store is no good
            return false;
        } else {
            // inside the limit of retained data so double check we have stats
            // in the period of interest.
            return eventStoreTable.doesStatisticExist(uniqueIdCache, statisticConfiguration, rollUpBitMask, period);
        }
    }

    /**
     * Looks in the store to see if the statistic name exists anywhere in the
     * table
     *
     * @param uniqueIdCache          The UID cache to use to resolve UIDs
     * @param statisticConfiguration The name of the statistic to look for
     * @return true if it exists
     */
    public boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                                      final StatisticConfiguration statisticConfiguration) {
        return eventStoreTable.doesStatisticExist(uniqueIdCache, statisticConfiguration);
    }

    private long calculatePurgeUpToTimeMs(final long startTime) {
        final int rowKeyIntervalsToRetain = getPurgeRetentionIntervals();

        // round start time down to the last row key interval
        final long roundedNow = new Long(startTime / timeInterval.rowKeyInterval()) * timeInterval.rowKeyInterval();

        // subtract the desired number of row key intervals to retain
        final long purgeUpToTimeMs = roundedNow - (timeInterval.rowKeyInterval() * rowKeyIntervalsToRetain);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "rowKeyIntervalsToRetain: {}, startTime: {}, roundedNow: {}, purgeUpToTimeMs: {}",
                    rowKeyIntervalsToRetain, DateUtil.createNormalDateTimeString(startTime),
                    DateUtil.createNormalDateTimeString(roundedNow),
                    DateUtil.createNormalDateTimeString(purgeUpToTimeMs));
        }

        return purgeUpToTimeMs;
    }

    public void purgeStatisticDataSourceData(final UniqueIdCache uniqueIdCache,
                                             final List<StatisticConfiguration> statisticConfigurations,
                                             final PurgeMode purgeMode) {
        final long startTime = getCurrentTimeMs();

        final Consumer<StatisticConfiguration> consumer;
        if (PurgeMode.OUTSIDE_RETENTION.equals(purgeMode)) {
            consumer = (statisticStore) -> {
                final long purgeUpToTimeMs = calculatePurgeUpToTimeMs(startTime);
                LOGGER.info(
                        "Purging store [{}] with data source count [{}] and row key interval size [{}].  Purging up to [{}]",
                        timeInterval.longName(), statisticConfigurations.size(), this.timeInterval.getRowKeyIntervalAsString(),
                        DateUtil.createNormalDateTimeString(purgeUpToTimeMs));

                // generate roll up masks based on the number of tags on the
                // stat and whether roll ups are enabled or
                // not
                final Set<RollUpBitMask> bitMasks = RollUpBitMask
                        .getRollUpBitMasks(statisticStore.getRollUpType().equals(StatisticRollUpType.ALL)
                                ? statisticStore.getFieldNames().size() : 0);

                for (final RollUpBitMask rollUpBitMask : bitMasks) {
                    eventStoreTable.purgeUntilTime(uniqueIdCache, statisticStore, rollUpBitMask,
                            purgeUpToTimeMs);
                }

            };
        } else {
            consumer = (statisticStore) -> {
                eventStoreTable.purgeAll(uniqueIdCache, statisticStore);
            };
        }

        for (final StatisticConfiguration statisticConfiguration : statisticConfigurations) {
            try {
                consumer.accept(statisticConfiguration);
            } catch (final CacheException ce) {
                if (ce.getMessage().contains(statisticConfiguration.getName())) {
                    LOGGER.info("Unable to purge statistics for [{}] in store [{}] due to there being no entry in the UID cache for it.  With no entry in the cache there should be no statistics to purge",
                            statisticConfiguration.getName(), timeInterval.longName());
                } else {
                    throw ce;
                }
            }
        }

        final long runTime = System.currentTimeMillis() - startTime;

        LOGGER.info(() -> String.format("Purged event store [%s] in %.2f mins", timeInterval.longName(),
                new Double(runTime / 1000d / 60d)));
    }

    public boolean isTimeInsidePurgeRetention(final long timeMs) {
        final long purgeUpToTimeMs = calculatePurgeUpToTimeMs(getCurrentTimeMs());

        return timeMs >= purgeUpToTimeMs;
    }

    /**
     * To aid with running junits, i.e. to allow us to hard code the time in
     * tests
     */
    long getCurrentTimeMs() {
        return System.currentTimeMillis();
    }

    public EventStoreTable getEventStoreTable() {
        return eventStoreTable;
    }

    @Override
    public String toString() {
        return "EventStore [timeInterval=" + timeInterval + "]";
    }

    private int getPurgeRetentionIntervals() {
        final String newPropValString = propertyService.getPropertyOrThrow(purgeRetentionPeriodsPropertyKey);

        // optimisation to avoid the parseInt on repeated calls
        if (newPropValString.equals(purgeIntervalsPropertyValStr)) {
            return purgeIntervalsPropertyVal;
        } else {
            purgeIntervalsPropertyVal = Integer.parseInt(newPropValString);
            return purgeIntervalsPropertyVal;
        }
    }
}
