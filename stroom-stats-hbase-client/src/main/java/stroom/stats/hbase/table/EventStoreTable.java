

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
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ValueCellValue;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.streams.aggregation.AggregatedEvent;

import java.util.List;

public interface EventStoreTable extends GenericTable {


    /**
     * Adds the passed aggregated events into the table, merging them with any existing aggregates
     * @param statisticType The type of the events being aggregated (COUNT|VALUE)
     * @param aggregatedEvents A list of events that may or may not be the product of multiple events
     *                         aggregated together
     */
    void addAggregatedEvents(final StatisticType statisticType, final List<AggregatedEvent> aggregatedEvents);

    /**
     * Adds 1 to the value in the cell defined by the passed cellQualifier. Does
     * an increment on cell, incrementing it by 1. Increment operation is atomic
     * so should be safe from other threads. The add operation is buffered for
     * performance.
     *
     * @param countRowData
     *            The cell to add to the data store
     * @param isForcedFlushToDisk
     *            Writes the count directly to the data store without buffering
     */
    //TODO remove
//    void bufferedAddCount(final CountRowData countRowData, final boolean isForcedFlushToDisk);


    //TODO remove
//    void addMultipleCounts(final Map<RowKey, List<CountCellIncrementHolder>> rowChanges);

    /**
     * Aggregates the passed value object into the cell with qualifier
     * cellQualifier. The value is added to the existing cell value, the counter
     * in the cell is incremented and the min/max
     *
     * The value cells contain a composite value that is made up of a count and
     * the current aggregated value (see {@link ValueCellValue}). Adding a value
     * increments that count by one and adds to the existing aggregated value.
     *
     * @param cellQualifier
     *            The cell qualifier to determine where the value is put
     * @param valueCellValue
     *            The value object containing the possibly already aggregated
     *            values
     */
    //TODO remove
//    void addValue(CellQualifier cellQualifier, ValueCellValue valueCellValue);

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

    /**
     * @return The number of items currently on the put buffer, zero if not
     *         initialised
     */
    //TODO remove
//    int getPutBufferCount(boolean isDeepCount);

    /**
     * @return Count of the number of permits currently available for batch put
     *         tasks. For use in monitoring or debugging only.
     */
    //TODO remove
//    int getAvailableBatchPutTaskPermits();

    long getCellsPutCount(StatisticType statisticType);
}
