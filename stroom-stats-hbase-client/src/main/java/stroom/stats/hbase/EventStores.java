

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
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.Period;
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
import java.time.Instant;
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
    private EventStore findBestFit(final SearchStatisticsCriteria criteria, final StatisticConfiguration statisticConfiguration) {
        // Try to determine which store holds the data precision we will need to
        // serve this query.
        EventStoreTimeIntervalEnum bestFitInterval;

        Period effectivePeriod = buildEffectivePeriod(criteria);

        final long periodMillis = effectivePeriod.duration();

        if (periodMillis == 0) {
            throw new RuntimeException(String.format("Not possible to calculate a best fit store for a zero length time period"));
        }

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
        // that. To support queries with no date term we will always return the largest store as a last resort
        //irrespective of the purge retention
        while (bestFitStore.isTimeInsidePurgeRetention(effectivePeriod.getFrom()) == false) {
            EventStore nextBiggestStore = eventStoreMap.get(EventStoreTimeIntervalHelper.getNextBiggest(bestFitStore.getTimeInterval()));

            if (nextBiggestStore == null) {
                // there is no next biggest so no point continuing
                break;
            }
            bestFitStore = nextBiggestStore;
        }
        final EventStoreTimeIntervalEnum bestFitBasedOnRetention = bestFitStore != null ? bestFitStore.getTimeInterval() : null;
        bestFitInterval = bestFitBasedOnRetention;

        LOGGER.info("Using event store [{}] for search.  Best fit based on: period - [{}], data source - [{}] & retention - [{}]",
                nullSafePrintInterval(bestFitInterval),
                nullSafePrintInterval(bestFitBasedOnPeriod),
                nullSafePrintInterval(bestFitBasedOnDataSource),
                nullSafePrintInterval(bestFitBasedOnRetention));

        return bestFitStore;
    }

    private String nullSafePrintInterval(EventStoreTimeIntervalEnum interval) {
        if (interval == null) {
            return "NULL";
        } else {
            return interval.longName();
        }
    }

    /**
     * If parts are missing from the period then constrain it using the epoch and/or now
     */
    private Period buildEffectivePeriod(final SearchStatisticsCriteria criteria) {
        Long from = criteria.getPeriod().getFrom();
        Long to = criteria.getPeriod().getTo();
        return new Period(from != null ? from : 0, to != null ? to : Instant.now().toEpochMilli());

    }

    public StatisticDataSet getStatisticsData(final SearchStatisticsCriteria criteria,
                                              final StatisticConfiguration statisticConfiguration) {

        LOGGER.info("Searching statistics store with criteria: {}", criteria);
        // Make sure a period has been requested.
        if (criteria.getPeriod() == null) {
            throw new StatisticsException("Results must be requested from a given period");
        }

        // Try to determine which store holds the data precision we will need to
        // serve this query.
        EventStore bestFit = criteria.getInterval()
                .flatMap(interval -> Optional.of(eventStoreMap.get(interval)))
                .orElseGet(() ->
                        findBestFit(criteria, statisticConfiguration)
                );

        LOGGER.debug("Using event store: " + bestFit.getTimeInterval().longName());

        StatisticDataSet statisticDataSet;


        // Get results from the selected event store.
        statisticDataSet = bestFit.getStatisticsData(uidCache, statisticConfiguration,  criteria);

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
