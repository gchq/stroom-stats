

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
import stroom.query.DateExpressionParser;
import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.Query;
import stroom.query.api.SearchRequest;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FilterTermsTreeBuilder;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.Period;
import stroom.stats.common.Range;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.common.StatisticConfigurationValidator;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.CustomRollUpMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is the entry point for all interactions with the HBase backed statistics store, e.g.
 * putting events, searching for data, purging data etc.
 */
public class HBaseStatisticsService implements StatisticsService {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseStatisticsService.class);

    public static final String ENGINE_NAME = "hbase";

    private static final List<ExpressionTerm.Condition> SUPPORTED_DATE_CONDITIONS = Arrays
            .asList(new ExpressionTerm.Condition[]{
                    ExpressionTerm.Condition.BETWEEN,
                    ExpressionTerm.Condition.EQUALS,
                    ExpressionTerm.Condition.LESS_THAN,
                    ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO,
                    ExpressionTerm.Condition.GREATER_THAN,
                    ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO});

    private final EventStores eventStores;
    private final StatisticConfigurationValidator statisticConfigurationValidator;
    private final StatisticConfigurationService statisticConfigurationService;
    private final StroomPropertyService propertyService;

    @Inject
    public HBaseStatisticsService(final StatisticConfigurationService statisticConfigurationService,
                                  final StatisticConfigurationValidator statisticConfigurationValidator,
                                  final EventStores eventStores,
                                  final StroomPropertyService propertyService) {

        this.statisticConfigurationValidator = statisticConfigurationValidator;
        this.statisticConfigurationService = statisticConfigurationService;
        this.propertyService = propertyService;

        LOGGER.debug("Initialising: {}", this.getClass().getCanonicalName());

        this.eventStores = eventStores;
    }

    protected static FindEventCriteria buildCriteria(final SearchRequest searchRequest,
                                                     final StatisticConfiguration statisticConfiguration) {
        LOGGER.trace(() -> String.format("buildCriteria called for statisticConfiguration ", statisticConfiguration));

        Preconditions.checkNotNull(searchRequest);
        Preconditions.checkNotNull(statisticConfiguration);
        Query query = searchRequest.getQuery();
        Preconditions.checkNotNull(query);


        // Get the current time in millis since epoch.
        final long nowEpochMilli = System.currentTimeMillis();

        // object looks a bit like this
        // AND
        // Date Time between 2014-10-22T23:00:00.000Z,2014-10-23T23:00:00.000Z

        final ExpressionOperator topLevelExpressionOperator = searchRequest.getQuery().getExpression();

        if (topLevelExpressionOperator == null || topLevelExpressionOperator.getOp().getDisplayValue() == null) {
            throw new IllegalArgumentException(
                    "The top level operator for the query must be one of [" + ExpressionOperator.Op.values() + "]");
        }

        final List<ExpressionItem> childExpressions = topLevelExpressionOperator.getChildren();
        int validDateTermsFound = 0;
        int dateTermsFound = 0;

        // Identify the date term in the search criteria. Currently we must have
        // a exactly one date term due to the way we have start/stop rowkeys
        // It may be possible to instead scan with just the statName and mask prefix and
        // then add date handling logic into the custom filter, but this will likely be slower
        ExpressionTerm dateTerm = null;
        if (childExpressions != null) {
            for (final ExpressionItem expressionItem : childExpressions) {
                if (expressionItem instanceof ExpressionTerm) {
                    final ExpressionTerm expressionTerm = (ExpressionTerm) expressionItem;

                    if (expressionTerm.getField() == null) {
                        throw new IllegalArgumentException("Expression term does not have a field specified");
                    }

                    if (expressionTerm.getField().equals(StatisticConfiguration.FIELD_NAME_DATE_TIME)) {
                        dateTermsFound++;

                        if (SUPPORTED_DATE_CONDITIONS.contains(expressionTerm.getCondition())) {
                            dateTerm = expressionTerm;
                            validDateTermsFound++;
                        }
                    }
                } else if (expressionItem instanceof ExpressionOperator) {
                    if (((ExpressionOperator) expressionItem).getOp().getDisplayValue() == null) {
                        throw new IllegalArgumentException(
                                "An operator in the query is missing a type, it should be one of " + ExpressionOperator.Op.values());
                    }
                }
            }
        }

        //TODO Factor out this query validation
        // ensure we have a date term
        if (dateTermsFound != 1 || validDateTermsFound != 1) {
            throw new UnsupportedOperationException(
                    "Search queries on the statistic store must contain one term using the '"
                            + StatisticConfiguration.FIELD_NAME_DATE_TIME
                            + "' field with one of the following conditions [" + SUPPORTED_DATE_CONDITIONS.toString()
                            + "].  Please amend the query");
        }

        // ensure the value field is not used in the query terms
        query.getExpression().getChildren().forEach(expressionItem -> {
            if (expressionItem instanceof ExpressionTerm
                    && ((ExpressionTerm) expressionItem).getValue().contains(StatisticConfiguration.FIELD_NAME_VALUE)) {
                throw new UnsupportedOperationException("Search queries containing the field '"
                        + StatisticConfiguration.FIELD_NAME_VALUE + "' are not supported.  Please remove it from the query");
            }
        });

        // if we have got here then we have a single BETWEEN date term, so parse
        // it.
        final Range<Long> range = extractRange(dateTerm, searchRequest.getDateTimeLocale(), nowEpochMilli);

        final List<ExpressionTerm> termNodesInFilter = new ArrayList<>();

        findAllTermNodes(topLevelExpressionOperator, termNodesInFilter);

        final Set<String> rolledUpFieldNames = new HashSet<>();

        for (final ExpressionTerm term : termNodesInFilter) {
            // add any fields that use the roll up marker to the black list. If
            // somebody has said user=* then we do not
            // want that in the filter as it will slow it down. The fact that
            // they have said user=* means it will use
            // the statistic name appropriate for that rollup, meaning the
            // filtering is built into the stat name.
            if (term.getValue().equals(RollUpBitMask.ROLL_UP_TAG_VALUE)) {
                rolledUpFieldNames.add(term.getField());
            }
        }

        if (!rolledUpFieldNames.isEmpty()) {
            if (statisticConfiguration.getRollUpType().equals(StatisticRollUpType.NONE)) {
                throw new UnsupportedOperationException(
                        "Query contains rolled up terms but the Statistic Data Source does not support any roll-ups");
            } else if (statisticConfiguration.getRollUpType().equals(StatisticRollUpType.CUSTOM)) {
                if (!statisticConfiguration.isRollUpCombinationSupported(rolledUpFieldNames)) {
                    throw new UnsupportedOperationException(String.format(
                            "The query contains a combination of rolled up fields %s that is not in the list of custom roll-ups for the statistic data source",
                            rolledUpFieldNames));
                }
            }
        }

        // Date Time is handled spearately to the the filter tree so ignore it
        // in the conversion
        final Set<String> blackListedFieldNames = new HashSet<>();
        blackListedFieldNames.addAll(rolledUpFieldNames);
        blackListedFieldNames.add(StatisticConfiguration.FIELD_NAME_DATE_TIME);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder
                .convertExpresionItemsTree(topLevelExpressionOperator, blackListedFieldNames);

        final FindEventCriteria criteria = FindEventCriteria.builder(new Period(range.getFrom(), range.getTo()), statisticConfiguration.getName())
                .setFilterTermsTree(filterTermsTree)
                .setRolledUpFieldNames(rolledUpFieldNames)
                .build();

        LOGGER.info("Searching statistics store with criteria: {}", criteria);
        return criteria;
    }

    // TODO could go futher up the chain so is store agnostic
    static RolledUpStatisticEvent generateTagRollUps(final StatisticEvent event,
                                                     final StatisticConfiguration statisticConfiguration) {
        RolledUpStatisticEvent rolledUpStatisticEvent = null;

        final int eventTagListSize = event.getTagList().size();

        final StatisticRollUpType rollUpType = statisticConfiguration.getRollUpType();

        if (eventTagListSize == 0 || StatisticRollUpType.NONE.equals(rollUpType)) {
            rolledUpStatisticEvent = new RolledUpStatisticEvent(event);
        } else if (StatisticRollUpType.ALL.equals(rollUpType)) {
            final List<List<StatisticTag>> tagListPerms = generateStatisticTagPerms(event.getTagList(),
                    RollUpBitMask.getRollUpPermutationsAsBooleans(eventTagListSize));

            // wrap the original event along with the perms list
            rolledUpStatisticEvent = new RolledUpStatisticEvent(event, tagListPerms);

        } else if (StatisticRollUpType.CUSTOM.equals(rollUpType)) {
            final Set<List<Boolean>> perms = new HashSet<>();
            for (final CustomRollUpMask mask : statisticConfiguration.getCustomRollUpMasks()) {
                final RollUpBitMask rollUpBitMask = RollUpBitMask.fromTagPositions(mask.getRolledUpTagPositions());

                perms.add(rollUpBitMask.getBooleanMask(eventTagListSize));
            }
            final List<List<StatisticTag>> tagListPerms = generateStatisticTagPerms(event.getTagList(), perms);

            rolledUpStatisticEvent = new RolledUpStatisticEvent(event, tagListPerms);
        }

        return rolledUpStatisticEvent;
    }

    private static Range<Long> extractRange(final ExpressionTerm dateTerm,
                                            final String timeZoneId,
                                            final long nowEpochMilli) {

        final String[] dateArr = dateTerm.getValue().split(",");


        List<Long> timeParts = Arrays.stream(dateArr)
                .map(dateStr -> parseDateTime("dateTime", dateStr, timeZoneId, nowEpochMilli)
                        .map(zonedDateTime -> zonedDateTime.toInstant().toEpochMilli())
                        .orElse(null))
                .collect(Collectors.toList());

//        Long dateTime0 = parseDateTime("from", dateArr[0], timeZoneId, nowEpochMilli)
//                .map(zonedDateTime -> zonedDateTime.toInstant().toEpochMilli())
//                .orElse(null);
//        // add one to make it exclusive
//        Long dateTime1 = parseDateTime("to", dateArr[1], timeZoneId, nowEpochMilli)
//                .map(zonedDateTime -> zonedDateTime.toInstant().toEpochMilli() + 1)
//                .orElse(null);

        if (dateTerm.getCondition().equals(ExpressionTerm.Condition.BETWEEN)) {
            if (timeParts.size() != 2) {
                throw new RuntimeException("BETWEEN DateTime term must have two parts, term: " + dateTerm.toString());
            }
        } else {
            if (timeParts.size() != 2) {
                throw new RuntimeException("BETWEEN DateTime term must have two parts, term: " + dateTerm.toString());
            }
        }

        Long fromMs = null;
        Long toMs = null;

        switch (dateTerm.getCondition()) {
            case EQUALS:
                fromMs = timeParts.get(0);
                toMs = timeParts.get(0);
                break;
            case BETWEEN:
                fromMs = timeParts.get(0);
                toMs = timeParts.get(1);
                break;
            case LESS_THAN:
                toMs = timeParts.get(0);
                break;
            case LESS_THAN_OR_EQUAL_TO:
                toMs = timeParts.get(0) + 1; //make it exclusive
                break;
            case GREATER_THAN:
                fromMs = timeParts.get(0);
                break;
            case GREATER_THAN_OR_EQUAL_TO:
                toMs = timeParts.get(0) + 1; //make it exclusive
                break;
            default:
                throw new RuntimeException("Should not have got here, unexpected condition " + dateTerm.getCondition());
        }

        final Range<Long> range = new Range<>(fromMs, toMs);

        return range;
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

    /**
     * TODO: This is a bit simplistic as a user could create a filter that said
     * user=user1 AND user='*' which makes no sense. At the moment we would
     * assume that the user tag is being rolled up so user=user1 would never be
     * found in the data and thus would return no data.
     */
    public static RollUpBitMask buildRollUpBitMaskFromCriteria(final FindEventCriteria criteria,
                                                               final StatisticConfiguration statisticConfiguration) {
        final Set<String> rolledUpTagsFound = criteria.getRolledUpFieldNames();

        final RollUpBitMask result;

        if (rolledUpTagsFound.size() > 0) {
            final List<Integer> rollUpTagPositionList = new ArrayList<>();

            for (final String tag : rolledUpTagsFound) {
                final Integer position = statisticConfiguration.getPositionInFieldList(tag);
                if (position == null) {
                    throw new RuntimeException(String.format("No field position found for tag %s", tag));
                }
                rollUpTagPositionList.add(position);
            }
            result = RollUpBitMask.fromTagPositions(rollUpTagPositionList);

        } else {
            result = RollUpBitMask.ZERO_MASK;
        }
        return result;
    }

    /**
     * Recursive method to populates the passed list with all enabled
     * {@link ExpressionTerm} nodes found in the tree.
     */
    public static void findAllTermNodes(final ExpressionItem node, final List<ExpressionTerm> termsFound) {
        // Don't go any further down this branch if this node is disabled.
        if (node.enabled()) {
            if (node instanceof ExpressionTerm) {
                final ExpressionTerm termNode = (ExpressionTerm) node;

                termsFound.add(termNode);

            } else if (node instanceof ExpressionOperator) {
                for (final ExpressionItem childNode : ((ExpressionOperator) node).getChildren()) {
                    findAllTermNodes(childNode, termsFound);
                }
            }
        }
    }


    @Override
    public void putAggregatedEvents(final StatisticType statisticType,
                                    final EventStoreTimeIntervalEnum interval,
                                    final Map<StatKey, StatAggregate> aggregatedEvents) {
        //TODO
        //Think we need to change the javadoc on this method to state that message should belong to the same stat type
        //and interval. This is is to save having to group them by type/interval again if they we already divided up
        //that way in the topics.
        //Change EventStores to take a list of AggregatedEvents
        //Change EventStore to take a list of AggregatedEvents
        //in HBaseEventStoreTable change to take a list of AggregatedEvents and convert these into CellQualifiers
        //revisit buffering in HBEST as we want to consume a batch, put that batch then commit kafka
        //kafka partitioning should mean similarity between stat keys in the batch
        eventStores.putAggregatedEvents(statisticType, interval, aggregatedEvents);
    }

    @Override
    public StatisticDataSet searchStatisticsData(final SearchRequest searchRequest, final StatisticConfiguration dataSource) {
        final FindEventCriteria criteria = buildCriteria(searchRequest, dataSource);
        return eventStores.getStatisticsData(criteria, dataSource);
    }

    // @Override
    // public void refreshMetadata() {
    // statStoreMetadataService.refreshMetadata();
    // }

    @Override
    public List<String> getValuesByTag(final String tagName) {
        // TODO This will be used for providing a dropdown of known values in
        // the UI
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

    private static Optional<ZonedDateTime> parseDateTime(final String type, final String value, final String timeZoneId, final long nowEpochMilli) {
        try {
            return DateExpressionParser.parse(value, timeZoneId, nowEpochMilli);
        } catch (final Exception e) {
            throw new RuntimeException("DateTime term has an invalid '" + type + "' value of '" + value + "'");
        }
    }
}
