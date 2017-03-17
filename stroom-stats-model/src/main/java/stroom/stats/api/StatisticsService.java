

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

package stroom.stats.api;

import stroom.query.api.Query;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;

import java.util.List;

public interface StatisticsService {

    /**
     * Puts multiple aggregated events into the appropriate store.
     * @param statisticType The type of ALL the events in the aggregatedEvents
     * @param interval The time interval that the aggregatedEvents have been aggregated to
     * @param aggregatedEvents An event that has been aggregated from zero-many source events
     */
    void putAggregatedEvents(final StatisticType statisticType,
                             final EventStoreTimeIntervalEnum interval,
                             final List<AggregatedEvent> aggregatedEvents);

    /**
     * @param statisticEvents Puts multiple statistic events to the store. The statistic events can be for different
     *                        statistic configurations
     *
     * @return
     */
    boolean putEvents(final List<StatisticEvent> statisticEvents);

    /**
     * @param statisticConfiguration The statistic configuration for ALL statisticEvent objects in the list. This will not be validated.
     * @param statisticEvents List of statisticEvent objects to put to the store
     * @return
     */
    boolean putEvents(final StatisticConfiguration statisticConfiguration, final List<StatisticEvent> statisticEvents);

    StatisticDataSet searchStatisticsData(final Query query, StatisticConfiguration statisticConfiguration);

    /**
     * For a given statistic tag name, it returns all known values existing in
     * the statistic store
     *
     * @param tagName
     *            The statistic tag name to search for
     * @return A list of values associated with the given statistic tag name
     */
    List<String> getValuesByTag(String tagName);

    /**
     * For a given statistic tag name and part of a value, it returns all known
     * values existing in the statistic store that match
     *
     * @param tagName
     *            The statistic tag name to search for
     * @param partialValue
     *            A contiguous part of a statistic tag value which should be
     *            contained in any matches
     * @return Those statistic tag values that match both criteria
     */
    List<String> getValuesByTagAndPartialValue(String tagName, String partialValue);

    void purgeOldData(final List<StatisticConfiguration> statisticConfigurations);

    void purgeAllData(final List<StatisticConfiguration> statisticConfigurations);

    /**
     * Flushes all events currently held in memory down to the persistent event
     * store
     */
    void flushAllEvents();


    /**
     * Cleanly flushes any buffers or in memory stores. Intended for use in a
     * shutdown event
     */
    void shutdown();
}
