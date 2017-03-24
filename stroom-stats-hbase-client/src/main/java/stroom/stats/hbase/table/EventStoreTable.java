

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

import stroom.stats.api.StatisticType;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.Period;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;

import java.util.Map;

public interface EventStoreTable extends GenericTable {


    /**
     * Adds the passed aggregated events into the table, merging them with any existing aggregates.
     * It is expected that
     * @param statisticType The type of the events being aggregated (COUNT|VALUE)
     * @param aggregatedEvents A list of events that may or may not be the product of multiple events
     *                         aggregated together
     */
    void addAggregatedEvents(final StatisticType statisticType,
                             final Map<StatKey, StatAggregate> aggregatedEvents);


    StatisticDataSet getStatisticsData(final UniqueIdCache uniqueIdCache,
                                       final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask,
                                       final FindEventCriteria criteria);

    /**
     * Looks in the store to see if the statistic name exists anywhere in the
     * table
     *
     * @param uniqueIdCache
     *            The UID cache to use to resolve UIDs
     * @param statisticConfiguration
     *            The statistic to look for
     * @return true if it exists
     */
    boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                               final StatisticConfiguration statisticConfiguration);

    /**
     * Looks in the store to see if the statistic dataSource exists anywhere in
     * the table
     *
     * @param uniqueIdCache
     *            The UID cache to use to resolve UIDs
     * @param statisticConfiguration
     *            The statistic to look for
     * @param period
     *            The period to search over
     * @return true if it exists
     */
    boolean doesStatisticExist(final UniqueIdCache uniqueIdCache,
                               final StatisticConfiguration statisticConfiguration, RollUpBitMask rollUpBitMask, Period period);

    /**
     * Deletes selected rows from the event store matching the passed statistic
     * name and roll up mask. The passed purgeUpToTimeMs value will be converted
     * into a partial timestamp and any rows with that partial timestamp or
     * lower will be deleted
     *
     * @param uniqueIdCache
     *            The UID cache to use to resolve UIDs
     * @param statisticConfiguration
     *            The statisticConfiguration to purge data from
     * @param rollUpBitMask
     *            The object containing the bit mask to apply to the row key
     * @param purgeUpToTimeMs
     *            The time to purge data up to
     */
    void purgeUntilTime(final UniqueIdCache uniqueIdCache,
                        final StatisticConfiguration statisticConfiguration, final RollUpBitMask rollUpBitMask,
                        final long purgeUpToTimeMs);

    void purgeAll(final UniqueIdCache uniqueIdCache, final StatisticConfiguration statisticConfiguration);

//    void flushPutBuffer();

    void shutdown();

    long getCellsPutCount(StatisticType statisticType);

    EventStoreTimeIntervalEnum getInterval();
}
