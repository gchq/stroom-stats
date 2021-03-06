

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

import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is the entry point for all interactions with the HBase backed statistics store, e.g.
 * putting events, searching for data, purging data etc.
 */
public class HBaseStatisticsService implements StatisticsService {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseStatisticsService.class);

    public static final String ENGINE_NAME = "hbase";


    private final EventStores eventStores;

    @Inject
    public HBaseStatisticsService(final EventStores eventStores) {

        LOGGER.debug("Initialising: {}", this.getClass().getCanonicalName());

        this.eventStores = eventStores;
    }

    private static List<List<StatisticTag>> generateStatisticTagPerms(final List<StatisticTag> eventTags,
                                                                      final Set<List<Boolean>> perms) {
        final List<List<StatisticTag>> tagListPerms = new ArrayList<>();
        final int eventTagListSize = eventTags.size();

        for (final List<Boolean> perm : perms) {
            final List<StatisticTag> tags = new ArrayList<>();
            for (int i = 0; i < eventTagListSize; i++) {
                if (perm.get(i).booleanValue() == true) {
                    // true means a rolled up tag so create a new tag with the
                    // rolled up marker
                    tags.add(new StatisticTag(eventTags.get(i).getTag(), RollUpBitMask.ROLL_UP_TAG_VALUE));
                } else {
                    // false means not rolled up so use the existing tag's value
                    tags.add(eventTags.get(i));
                }
            }
            tagListPerms.add(tags);
        }
        return tagListPerms;
    }

    @Override
    public void putAggregatedEvents(final StatisticType statisticType,
                                    final EventStoreTimeIntervalEnum interval,
                                    final Map<StatEventKey, StatAggregate> aggregatedEvents) {

        eventStores.putAggregatedEvents(statisticType, interval, aggregatedEvents);
    }

    @Override
    public StatisticDataSet searchStatisticsData(final SearchStatisticsCriteria searchStatisticsCriteria,
                                                 final StatisticConfiguration statisticConfiguration) {

        return eventStores.getStatisticsData(searchStatisticsCriteria, statisticConfiguration);
    }

    @Override
    public List<String> getValuesByTag(final String tagName) {
        // TODO This will be used for providing a dropdown of known values in the UI
        throw new UnsupportedOperationException("Code waiting to be written");
    }

    @Override
    public List<String> getValuesByTagAndPartialValue(final String tagName, final String partialValue) {
        // TODO This will be used for auto-completion in the UI
        throw new UnsupportedOperationException("Code waiting to be written");
    }

    @Override
    public void purgeOldData(final List<StatisticConfiguration> statisticConfigurations) {
        eventStores.purgeOldData(statisticConfigurations);

    }

    @Override
    public void purgeAllData(final List<StatisticConfiguration> statisticConfigurations) {
        eventStores.purgeStatisticStore(statisticConfigurations);

    }

    @Override
    public void flushAllEvents() {
        eventStores.flushAllEvents();
    }

    @Deprecated
    public EventStores getEventStoresForTesting() {
        return eventStores;
    }

    @Override
    public void shutdown() {
        flushAllEvents();
    }

}
