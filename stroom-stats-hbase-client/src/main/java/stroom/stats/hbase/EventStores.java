

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

import stroom.stats.api.StatisticType;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.exception.StatisticsException;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
public class EventStores {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(EventStores.class);

    private final UniqueIdCache uidCache;
    private final EventStoreTableFactory eventStoreTableFactory;
    private final StroomPropertyService propertyService;

    //Map to hold an EventStore per granularity
    private final Map<EventStoreTimeIntervalEnum, EventStore> eventStoreMap = new EnumMap<>(
            EventStoreTimeIntervalEnum.class);

    //Map to hold a row key builder instance per granularity
    private final Map<EventStoreTimeIntervalEnum, RowKeyBuilder> cachedRowKeyBuilders = new EnumMap<>(
            EventStoreTimeIntervalEnum.class);

    @Inject
    public EventStores(final RowKeyCache rowKeyCache,
                       final UniqueIdCache uniqueIdCache,
                       final EventStoreTableFactory eventStoreTableFactory,
                       final StroomPropertyService propertyService) throws IOException {

        LOGGER.info("Initialising: {}", this.getClass().getCanonicalName());

        this.eventStoreTableFactory = eventStoreTableFactory;
        this.propertyService = propertyService;

        this.uidCache = uniqueIdCache;

        // set up an event store and cache a row key builder for each time
        // interval that we use
        for (final EventStoreTimeIntervalEnum timeIntervalEnum : EventStoreTimeIntervalEnum.values()) {
            addStore(timeIntervalEnum);

            //TODO need to get rid of this from here and keep it in our HBase... classes only
            cachedRowKeyBuilders.put(timeIntervalEnum,
                    CachedRowKeyBuilder.wrap(new SimpleRowKeyBuilder(uidCache, timeIntervalEnum), rowKeyCache));
        }
    }

    public Map<EventStoreTimeIntervalEnum, Long> getCellsPutCount(final StatisticType statisticType) {
        final Map<EventStoreTimeIntervalEnum, Long> map = new HashMap<>();

        for (final EventStore eventStore : eventStoreMap.values()) {
            map.put(eventStore.getTimeInterval(), eventStore.getCellsPutCount(statisticType));
        }
        return map;
    }

    // method used to extract an instance of the unique id cache in testing
    @Deprecated
    public UniqueIdCache getUniqueIdCache() {
        return uidCache;
    }

    /**
     * Flushes all events down to the datastore
     */
    public void flushAllEvents() {

        LOGGER.debug("flushAllEvents called");
        for (final EventStore eventStore : eventStoreMap.values()) {
            eventStore.flushAllEvents();
        }
    }

    private void addStore(final EventStoreTimeIntervalEnum interval) {
        final EventStore eventStore = new EventStore(this.uidCache, interval, eventStoreTableFactory, this.propertyService);

        // add the event store to the map of stores in use
        eventStoreMap.put(interval, eventStore);
    }

    /**
     * Puts a list of aggregated events into the appropriate event stores.
     * All aggregatedEvents must be for the same statisticType and interval.
     */
    public void putAggregatedEvents(final StatisticType statisticType,
                                    final EventStoreTimeIntervalEnum interval,
                                    final Map<StatKey, StatAggregate> aggregatedEvents) {

        eventStoreMap.get(interval)
                .putAggregatedEvents(statisticType, aggregatedEvents);
    }

    /**
     * @param criteria The criteria of the search
     * @return The best store to use based on the chosen bucket size (if there
     * is one), the period of the search and whether a store has any
     * data for that stat over that period. Null if there is no data in
     * any store for that period
     */
    private EventStore findBestFit(final FindEventCriteria criteria, final StatisticConfiguration statisticConfiguration) {
        // Try to determine which store holds the data precision we will need to
        // serve this query.
        EventStoreTimeIntervalEnum bestFitInterval;

        final long periodMillis = criteria.getPeriod().duration();

        // Work out which store to pull data from based on the period requested
        // and an optimum number of data points

        final int maxTimeIntervalsInPeriod = propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.SEARCH_MAX_INTERVALS_IN_PERIOD_PROPERTY_NAME);

        bestFitInterval = EventStoreTimeIntervalHelper.getBestFit(periodMillis, maxTimeIntervalsInPeriod);
        final EventStoreTimeIntervalEnum bestFitBasedOnPeriod = bestFitInterval;

        // the optimum may be finer than that configured for the data source so
        // if it is try the one from the data
        // source
        if (bestFitInterval.columnInterval() < statisticConfiguration.getPrecision()) {
            bestFitInterval = EventStoreTimeIntervalEnum.fromColumnInterval(statisticConfiguration.getPrecision());
        }
        final EventStoreTimeIntervalEnum bestFitBasedOnDataSource = bestFitInterval;

        EventStore bestFitStore = eventStoreMap.get(bestFitInterval);

        // take into account the purge retention. If the start point of the
        // search is outside the purge retention of a
        // given store then there is no point using that store as we would get
        // no/partial results back. It is possible
        // that the purge has not run or the purge retention has changed but
        // there is not a lot we can do to allow for
        // that.
        while (bestFitStore.isTimeInsidePurgeRetention(criteria.getPeriod().getFrom()) == false) {
            bestFitStore = eventStoreMap
                    .get(EventStoreTimeIntervalHelper.getNextBiggest(bestFitStore.getTimeInterval()));

            if (bestFitStore == null) {
                // there is no next biggest so no point continuing
                break;
            }

            if (bestFitStore.getTimeInterval().equals(EventStoreTimeIntervalHelper.getLargestInterval())) {
                // already at the biggest so break out and use this one and
                // return null as there is no point in running
                // the search if there is no data for this stat in any stores
                bestFitStore = null;
                break;
            }
        }
        final EventStoreTimeIntervalEnum bestFitBasedOnRetention = bestFitStore.getTimeInterval();

        LOGGER.info("Using event store [{}] for search.  Best fit based on: period - [{}], data source - [{}] & retention - [{}]",
                bestFitStore.getTimeInterval().longName(),
                bestFitBasedOnPeriod.longName(),
                bestFitBasedOnDataSource.longName(),
                bestFitBasedOnRetention.longName());

        return bestFitStore;
    }

    public StatisticDataSet getStatisticsData(final FindEventCriteria criteria,
                                              final StatisticConfiguration statisticConfiguration) {
        // Make sure a period has been requested.
        if (criteria.getPeriod() == null) {
            throw new StatisticsException("Results must be requested from a given period");
        }

        // Determine what duration we are requesting.
        final long duration = criteria.getPeriod().getTo() - criteria.getPeriod().getFrom();
        if (duration < 0) {
            throw new StatisticsException("The from time must be less than the to time");
        }

        // Try to determine which store holds the data precision we will need to
        // serve this query.
        EventStore bestFit;

        bestFit = findBestFit(criteria, statisticConfiguration);

        LOGGER.debug("using event store: " + (bestFit == null ? "NULL" : bestFit.getTimeInterval().longName()));

        StatisticDataSet statisticDataSet;

        final RollUpBitMask rollUpBitMask = HBaseStatisticsService.buildRollUpBitMaskFromCriteria(criteria,
                statisticConfiguration);

        if (bestFit == null) {
            LOGGER.debug("No stats exist for this period and criteria so returning an empty chartData object");
            final String statisticName = criteria.getStatisticName();
            statisticDataSet = new StatisticDataSet(statisticName, statisticConfiguration.getStatisticType());
        } else {
            // Get results from the selected event store.
            statisticDataSet = bestFit.getStatisticsData(uidCache, statisticConfiguration, rollUpBitMask, criteria);
        }

        return statisticDataSet;
    }

    //TODO implement alternative shutdown hook
//    @StroomShutdown(priority = 10)
    public void shutdown() {
        // explicitly flush any remaining events
        flushAllEvents();

        // Shutdown the Htable instances and then the HConfiguration
        for (final EventStore eventStore : eventStoreMap.values()) {
            eventStore.shutdown();
        }
    }

    /**
     * Finds the store with the finest granularity that contains the passed
     * statistic name as part of a row key
     *
     * @param statisticConfiguration The statistic to look for
     * @return The store with the finest granularity that contains the passed
     * stat name
     */
    public EventStore getFinestStore(final StatisticConfiguration statisticConfiguration) {
        Optional<EventStoreTimeIntervalEnum> interval = Optional.of(EventStoreTimeIntervalEnum.SECOND);

        EventStore finestStore = null;

        do {
            final EventStore store = eventStoreMap.get(interval.get());

            if (store == null) {
                throw new IllegalStateException("Don't have an eventStore for time interval: " + interval.get());
            }

            // see if the statname exists in this store
            if (store.doesStatisticExist(uidCache, statisticConfiguration)) {
                finestStore = store;
                break;
            }

            interval = EventStoreTimeIntervalHelper.getNextBiggest(interval.get());
        } while (interval.isPresent());

        return finestStore;
    }


    /**
     * Removes all statistics data outside of a retention period that
     * is configured on a per granularity basis
     *
     * @param statisticConfigurations The statistic configurations to purge from
     */
    public void purgeOldData(final List<StatisticConfiguration> statisticConfigurations) {
        LOGGER.info("HBase statistics purge to retention job started");

        final long startTime = System.currentTimeMillis();

        //Need to purge from each granularity
        for (final EventStore eventStore : eventStoreMap.values()) {
            eventStore.purgeStatisticDataSourceData(uidCache, statisticConfigurations, PurgeMode.OUTSIDE_RETENTION);
        }

        final long runTime = System.currentTimeMillis() - startTime;

        LOGGER.info(() ->
                String.format("HBase statistics purge to retention job completed in %.2f mins", runTime / 1000d / 60d));
    }

    /**
     * Removes all statistics data for the passed store
     *
     * @param statisticConfigurations The statistic configurations to purge from
     */
    public void purgeStatisticStore(final List<StatisticConfiguration> statisticConfigurations) {
        LOGGER.info("HBase statistics purge all job started");

        final long startTime = System.currentTimeMillis();

        //Need to purge from each granularity
        for (final EventStore eventStore : eventStoreMap.values()) {
            eventStore.purgeStatisticDataSourceData(uidCache, statisticConfigurations, PurgeMode.ALL);
        }

        final long runTime = System.currentTimeMillis() - startTime;

        LOGGER.info(() ->
                String.format("HBase statistics purge all job completed in %.2f mins", runTime / 1000d / 60d));
    }
}
