

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
import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
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

    public static RollUpBitMask buildRollUpBitMaskFromCriteria(final SearchStatisticsCriteria criteria,
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
                if (!fieldsOfInterest.isEmpty()) {
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

    @Override
    public void putAggregatedEvents(final StatisticType statisticType,
                                    final EventStoreTimeIntervalEnum interval,
                                    final Map<StatKey, StatAggregate> aggregatedEvents) {

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
