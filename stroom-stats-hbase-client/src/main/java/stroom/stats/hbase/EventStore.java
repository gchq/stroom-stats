

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

package stroom.stats.hbase;

import com.google.common.base.Preconditions;
import org.hibernate.cache.CacheException;
import stroom.stats.api.StatisticType;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.Period;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.table.EventStoreTable;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import java.util.*;
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
    public EventStore(final UniqueIdCache uidCache,
                      final EventStoreTimeIntervalEnum interval,
                      final EventStoreTableFactory eventStoreTableFactory,
                      final StroomPropertyService propertyService) {

        LOGGER.info("Initialising EventSore for interval {}", interval);

        this.eventStoreTable = eventStoreTableFactory.getEventStoreTable(interval);
        this.timeInterval = interval;
        this.propertyService = propertyService;
        this.purgeRetentionPeriodsPropertyKey = HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                + interval.name().toLowerCase();

        //TODO do we want to cache the conversion of AggregatedEvents to CellQualifiers?
        rowKeyBuilder = new SimpleRowKeyBuilder(uidCache, interval);
//        rowKeyBuilder = CachedRowKeyBuilder.wrap(new SimpleRowKeyBuilder(uidCache, interval), rowKeyCache));
    }

    static RollUpBitMask buildRollUpBitMaskFromCriteria(final SearchStatisticsCriteria criteria,
                                                        final StatisticConfiguration statisticConfiguration) {
        //attempt to roll up all fields not in the list of required fields
        //e.g. We have fields A B C D and A is part of the filter and A+C are needed in the results.
        //Thus a roll up combo of A * C * would be optimum.  If that doesn't exist then any combo
        //with A & C not rolled up and as many of the other fields rolled up as possible would be
        //preferable to non rolled up.  The middle ground between none rolled up and the optimum would ideally
        //take cardinality into account for best performance but we don't have that info.
        StatisticRollUpType rollUpType = Preconditions.checkNotNull(statisticConfiguration.getRollUpType());
        switch (rollUpType) {
            case NONE:
                return RollUpBitMask.ZERO_MASK;
            default:
                final List<String> requiredDynamicFields = criteria.getRequiredDynamicFields();

                final Set<String> fieldsInFilter = new HashSet<>();
                criteria.getFilterTermsTree().walkTree(node -> {
                    if (node instanceof FilterTermsTree.TermNode) {
                        fieldsInFilter.add(((FilterTermsTree.TermNode) node).getTag());
                    }
                });

                //get a set of all fields that feature in either the query filter or are required
                //in the result set
                final Set<String> fieldsOfInterest = fieldsInFilter;
                fieldsOfInterest.addAll(requiredDynamicFields);

                //now find all fields we don't care about as these are the ones to roll up
                final Set<String> fieldsNotOfInterest = new HashSet<>(statisticConfiguration.getFieldNames());
                fieldsNotOfInterest.removeAll(fieldsOfInterest);

                final RollUpBitMask optimumMask;
                if (!fieldsNotOfInterest.isEmpty()) {
                    final List<Integer> optimumRollUpPositionList = new ArrayList<>();

                    for (final String field : fieldsNotOfInterest) {
                        final Integer position = statisticConfiguration.getPositionInFieldList(field);
                        if (position == null) {
                            //should never happen
                            throw new RuntimeException(String.format("No field position found for tag %s", field));
                        }
                        optimumRollUpPositionList.add(position);
                    }
                    optimumMask = RollUpBitMask.fromTagPositions(optimumRollUpPositionList);

                } else {
                    optimumMask = RollUpBitMask.ZERO_MASK;
                }

                switch (rollUpType) {
                    case ALL:
                        //stat has all masks so just use the optimum
                        return optimumMask;
                    case CUSTOM:
                        //find the best available from what he have.  The custom masks should always include
                        //the zero mask so that will always be the last resort
                        return selectBestAvailableCustomMask(optimumMask, statisticConfiguration);
                    default:
                        throw new RuntimeException(String.format("Should never get here"));
                }
        }
    }

    /**
     * Compares the optimum mask against all of the custom masks of the stat config to find the one
     * that is as close as possible to the optimum.  The definition of closest is having as many fields
     * rolled up as possible, without rolling up any that are NOT rolled up in the optimum mask
     */
    private static RollUpBitMask selectBestAvailableCustomMask(RollUpBitMask optimumMask,
                                                               StatisticConfiguration statisticConfiguration) {

        Set<Integer> optimumRolledUpPositions = Preconditions.checkNotNull(optimumMask).getTagPositions();
        Set<RollUpBitMask> availableCustomMasks = Preconditions.checkNotNull(
                statisticConfiguration.getCustomRollUpMasksAsBitMasks());

        RollUpBitMask bestSoFar = null;
        int bestMatchCountSoFar = -1;
        for (RollUpBitMask customMask : availableCustomMasks) {

            //e.g. tags ABCD and we want A & B NOT rolled up.  Here we can pick from either of
            //the last two permutations. ('.'=not rolled up, '*'=rolled up)
            //A B C D
            //. . . .
            //. * . .
            //. . * .
            //. . . *

            int matchCount = optimumMask.getRollUpPositionMatchCount(customMask);

            if (matchCount > bestMatchCountSoFar) {
                //invalid if the custom mask contains any positions that are NOT rolled up in the optimum
                boolean isValid = !customMask.getTagPositions().stream()
                        .anyMatch(pos -> !optimumRolledUpPositions.contains(pos));

                if (isValid) {
                    //new bast so far
                    bestSoFar = customMask;
                    bestMatchCountSoFar = matchCount;

                    if (matchCount == optimumRolledUpPositions.size()) {
                        //will never get any better than this
                        break;
                    }
                }
            }
        }

        return Preconditions.checkNotNull(bestSoFar,
                "Should never get here, maybe the zero mask is not present in the set of custom masks");
    }

    /**
     * Puts a batch of aggregated events into the store
     */
    public void putAggregatedEvents(final StatisticType statisticType,
                                    final Map<StatKey, StatAggregate> aggregatedEvents) {

        eventStoreTable.addAggregatedEvents(statisticType, aggregatedEvents);
    }

    public void flushAllEvents() {
        LOGGER.debug("flushAllEvents called for store: {}", this.timeInterval);
        eventStoreTable.shutdown();
    }

    public long getCellsPutCount(final StatisticType statisticType) {
        return eventStoreTable.getCellsPutCount(statisticType);
    }

    public StatisticDataSet getStatisticsData(final UniqueIdCache uniqueIdCache,
                                              final StatisticConfiguration statisticConfiguration,
                                              final SearchStatisticsCriteria criteria) {

        final RollUpBitMask rollUpBitMask = buildRollUpBitMaskFromCriteria(criteria, statisticConfiguration);

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
