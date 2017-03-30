

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

import stroom.query.api.SearchRequest;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;

import java.util.List;
import java.util.Map;

public interface StatisticsService {

    /**
     * Puts multiple aggregated events into the appropriate store.
     *
     * @param statisticType    The type of ALL the events in the aggregatedEvents
     * @param interval         The time interval that ALL the aggregatedEvents have been aggregated to
     * @param aggregatedEvents An event that has been aggregated from zero-many source events
     */
    void putAggregatedEvents(final StatisticType statisticType,
                             final EventStoreTimeIntervalEnum interval,
                             final Map<StatKey, StatAggregate> aggregatedEvents);

    //TODO if we take a SearchRequest then we probably ought to return a SearchResponse object and
    //do the work that is currentlt done in HBclient in here

    /**
     * Perform a search of the statistic store. The requestedDynamicFields arg allows for optimisation
     * of the query such that any dynamic fields that are not required or not included as predicates
     * will be rolled up to improve query performance and reduce the returned data set.
     *
     * @param searchStatisticsCriteria Defines the search query
     * @param statisticConfiguration   The statistic to query against
     * @return An empty or populated {@link StatisticDataSet} object
     */
    StatisticDataSet searchStatisticsData(final SearchStatisticsCriteria searchStatisticsCriteria,
                                          final StatisticConfiguration statisticConfiguration);

//    /**
//     * Perform a search of the statistic store. No rolling up of data will be performed.
//     *
//     * @param searchRequest          Defines the search query
//     * @param statisticConfiguration The statistic to query against
//     * @return An empty or populated {@link StatisticDataSet} object
//     */
//    default StatisticDataSet searchStatisticsData(final SearchRequest searchRequest,
//                                                  final StatisticConfiguration statisticConfiguration) {
//
//        //supply all dynamic fields for this stat config so it uses the zero rollup mask, thus rolling up nothing
//        return searchStatisticsData(searchRequest,
//                statisticConfiguration.getFieldNames(),
//                statisticConfiguration);
//    }

    /**
     * For a given statistic tag name, it returns all known values existing in
     * the statistic store
     *
     * @param tagName The statistic tag name to search for
     * @return A list of values associated with the given statistic tag name
     */
    List<String> getValuesByTag(String tagName);

    /**
     * For a given statistic tag name and part of a value, it returns all known
     * values existing in the statistic store that match
     *
     * @param tagName      The statistic tag name to search for
     * @param partialValue A contiguous part of a statistic tag value which should be
     *                     contained in any matches
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
