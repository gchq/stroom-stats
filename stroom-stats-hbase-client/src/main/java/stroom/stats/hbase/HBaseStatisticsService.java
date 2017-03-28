

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
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.CustomRollUpMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
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

    private static final List<ExpressionTerm.Condition> SUPPORTED_DATE_CONDITIONS = Arrays.asList(
            ExpressionTerm.Condition.BETWEEN,
            ExpressionTerm.Condition.EQUALS,
            ExpressionTerm.Condition.LESS_THAN,
            ExpressionTerm.Condition.LESS_THAN_OR_EQUAL_TO,
            ExpressionTerm.Condition.GREATER_THAN,
            ExpressionTerm.Condition.GREATER_THAN_OR_EQUAL_TO);

    private static final List<String> SUPPORTED_QUERYABLE_STATIC_FIELDS = Arrays.asList(
            StatisticConfiguration.FIELD_NAME_DATE_TIME,
            StatisticConfiguration.FIELD_NAME_PRECISION);

    private final EventStores eventStores;

    @Inject
    public HBaseStatisticsService(final EventStores eventStores) {

        LOGGER.debug("Initialising: {}", this.getClass().getCanonicalName());

        this.eventStores = eventStores;
    }

    static FindEventCriteria buildCriteria(final SearchRequest searchRequest,
                                                     final StatisticConfiguration statisticConfiguration) {
        LOGGER.trace(() -> String.format("buildCriteria called for statisticConfiguration %s", statisticConfiguration));

        Preconditions.checkNotNull(searchRequest);
        Preconditions.checkNotNull(statisticConfiguration);
        Query query = searchRequest.getQuery();
        Preconditions.checkNotNull(query);

        // Get the current time in millis since epoch.
        final long nowEpochMilli = System.currentTimeMillis();

        // object looks a bit like this AND Date Time between 2014-10-22T23:00:00.000Z,2014-10-23T23:00:00.000Z
        final ExpressionOperator topLevelExpressionOperator = searchRequest.getQuery().getExpression();

        if (topLevelExpressionOperator == null || topLevelExpressionOperator.getOp().getDisplayValue() == null) {
            throw new IllegalArgumentException(
                    "The top level operator for the query must be one of [" + Arrays.toString(ExpressionOperator.Op.values()) + "]");
        }

        Optional<ExpressionTerm> optPrecisionTerm = validateSpecialTerm(
                topLevelExpressionOperator,
                StatisticConfiguration.FIELD_NAME_PRECISION);

        Optional<ExpressionTerm> optDateTimeTerm = validateSpecialTerm(
                topLevelExpressionOperator,
                StatisticConfiguration.FIELD_NAME_DATE_TIME);

        optDateTimeTerm.ifPresent(HBaseStatisticsService::validateDateTerm);
        Optional<EventStoreTimeIntervalEnum> optInterval = optPrecisionTerm.flatMap(precisionTerm ->
                Optional.of(validatePrecisionTerm(precisionTerm)));

        // if we have got here then we have a single BETWEEN date term, so parse it.
        final Range<Long> range = extractRange(optDateTimeTerm.orElse(null), searchRequest.getDateTimeLocale(), nowEpochMilli);

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

        // Some fields are handled separately to the the filter tree so ignore it in the conversion
        final Set<String> blackListedFieldNames = new HashSet<>();
        blackListedFieldNames.addAll(rolledUpFieldNames);
        blackListedFieldNames.add(StatisticConfiguration.FIELD_NAME_DATE_TIME);
        blackListedFieldNames.add(StatisticConfiguration.FIELD_NAME_PRECISION);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder
                .convertExpresionItemsTree(topLevelExpressionOperator, blackListedFieldNames);

        final FindEventCriteria.FindEventCriteriaBuilder criteriaBuilder = FindEventCriteria
                .builder(new Period(range.getFrom(), range.getTo()), statisticConfiguration.getName())
                .setFilterTermsTree(filterTermsTree)
                .setRolledUpFieldNames(rolledUpFieldNames);

        optInterval.ifPresent(criteriaBuilder::setInterval);

        return criteriaBuilder.build();
    }

    /**
     * Identify the date term in the search criteria. Currently we must have
     * a zero or one date terms due to the way we have start/stop rowkeys
     * It may be possible to instead scan with just the statName and mask prefix and
     * then add date handling logic into the custom filter, but this will likely be slower
     */
    private static void validateDateTerm(final ExpressionTerm dateTimeTerm) throws UnsupportedOperationException {
        Preconditions.checkNotNull(dateTimeTerm);
        Preconditions.checkNotNull(dateTimeTerm.getCondition());

        if (!SUPPORTED_DATE_CONDITIONS.contains(dateTimeTerm.getCondition())) {
            throw new UnsupportedOperationException(String.format("Date Time expression has an invalid condition %s, should be one of %s",
                    dateTimeTerm.getCondition(), SUPPORTED_DATE_CONDITIONS));
        }
    }

    private static EventStoreTimeIntervalEnum validatePrecisionTerm(final ExpressionTerm precisionTerm) {
        Preconditions.checkNotNull(precisionTerm);
        Preconditions.checkNotNull(precisionTerm.getValue());
        EventStoreTimeIntervalEnum interval;
        try {
            interval = EventStoreTimeIntervalEnum.valueOf(precisionTerm.getValue().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Precision term value %s is not a valid time interval",
                    precisionTerm.getValue()), e);
        }
        return interval;
    }

    /**
     * Ensure the the passed fieldName appears no more than once in the tree if it does appear
     * it must be directly below at root operator of AND. This is used for special fields that have to
     * be treated differently and cannot just be sprinkled round the boolean tree
     */
    private static Optional<ExpressionTerm> validateSpecialTerm(final ExpressionOperator rootOperator,
                                                                final String fieldName) throws RuntimeException {
        Preconditions.checkNotNull(rootOperator);
        Preconditions.checkNotNull(fieldName);

        ExpressionOperator.Op expectedRootOp = ExpressionOperator.Op.AND;
        int expectedPathLength = 2;
        List<List<ExpressionItem>> foundPaths = findFieldsInExpressionTree(rootOperator, fieldName);

        if (foundPaths.size() > 1) {
            throw new RuntimeException(String.format("Field %s is only allowed to appear once in the expression tree", fieldName));
        } else if (foundPaths.size() == 1) {
            List<ExpressionItem> path = foundPaths.get(0);

            if (rootOperator.getOp().equals(expectedRootOp) && path.size() == expectedPathLength) {
                return Optional.of((ExpressionTerm) path.get(expectedPathLength - 1));
            } else {
                throw new RuntimeException(String.format("Field %s must appear as a direct child to the root operator which must be %s",
                        fieldName, expectedRootOp));
            }
        } else {
            return Optional.empty();
        }
    }

    List<String> getQueryableFields(final StatisticConfiguration statisticConfiguration) {
        List<String> queryableFields = new ArrayList<>(statisticConfiguration.getFieldNames());
        queryableFields.addAll(SUPPORTED_QUERYABLE_STATIC_FIELDS);
        return queryableFields;
    }

    @Deprecated //now done in kafka streams
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
        Preconditions.checkNotNull(timeZoneId);
        Preconditions.checkArgument(nowEpochMilli > 0, "nowEpochMilli must be > 0");

        if (dateTerm == null) {
            return new Range<>(null, null);
        }

        //For a BETWEEN the date term str will look like 'xxxxxxxx,yyyyyyyyy' but for all others will
        //just be a single date string
        List<Long> timeParts = Arrays.stream(dateTerm.getValue().split(","))
                .map(dateStr -> parseDateTime("dateTime", dateStr, timeZoneId, nowEpochMilli)
                        .map(zonedDateTime -> zonedDateTime.toInstant().toEpochMilli())
                        .orElse(null))
                .collect(Collectors.toList());

        if (dateTerm.getCondition().equals(ExpressionTerm.Condition.BETWEEN)) {
            if (timeParts.size() != 2) {
                throw new RuntimeException("BETWEEN DateTime term must have two parts, term: " + dateTerm.toString());
            }
            if (timeParts.get(0) > timeParts.get(1)) {
                throw new RuntimeException("The first time part should be before the second time part, term: " + dateTerm.toString());
            }
        } else {
            if (timeParts.size() != 1) {
                throw new RuntimeException(String.format("%s DateTime term must have just one part, term: %s", dateTerm.getCondition(), dateTerm.toString()));
            }
        }

        Long fromMs = null;
        Long toMs = null;

        switch (dateTerm.getCondition()) {
            case EQUALS:
                fromMs = timeParts.get(0);
                toMs = timeParts.get(0) + 1; //make it exclusive
                break;
            case BETWEEN:
                fromMs = timeParts.get(0);
                toMs = timeParts.get(1) + 1; //make it exclusive
                break;
            case LESS_THAN:
                toMs = timeParts.get(0); //already exclusive
                break;
            case LESS_THAN_OR_EQUAL_TO:
                toMs = timeParts.get(0) + 1; //make it exclusive
                break;
            case GREATER_THAN:
                fromMs = timeParts.get(0) + 1; //make it exclusive
                break;
            case GREATER_THAN_OR_EQUAL_TO:
                fromMs = timeParts.get(0);
                break;
            default:
                throw new RuntimeException("Should not have got here, unexpected condition " + dateTerm.getCondition());
        }

        return new Range<>(fromMs, toMs);
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

    private static Optional<ZonedDateTime> parseDateTime(final String type, final String value, final String timeZoneId, final long nowEpochMilli) {
        try {
            return DateExpressionParser.parse(value, timeZoneId, nowEpochMilli);
        } catch (final Exception e) {
            throw new RuntimeException("DateTime term has an invalid '" + type + "' value of '" + value + "'");
        }
    }

    private static List<List<ExpressionItem>> findFieldsInExpressionTree(final ExpressionItem rootItem,
                                                                         final String targetFieldName) {

        List<List<ExpressionItem>> foundPaths = new ArrayList<>();
        List<ExpressionItem> currentParents = new ArrayList<>();
        currentParents.add(rootItem);
        walkExpressionTree(rootItem, targetFieldName, currentParents, foundPaths);
        return foundPaths;
    }

    private static void walkExpressionTree(final ExpressionItem item,
                                           final String targetFieldName,
                                           final List<ExpressionItem> currentParents,
                                           final List<List<ExpressionItem>> foundPaths) {

        if (item instanceof ExpressionTerm) {
            ExpressionTerm term = (ExpressionTerm) item;
            Preconditions.checkArgument(term.getField() != null);
            if (term.getField().equals(targetFieldName) && term.enabled()) {
                List<ExpressionItem> path = new ArrayList<>(currentParents);
                path.add(item);
                foundPaths.add(path);
            }
        } else if (item instanceof ExpressionOperator) {
            ExpressionOperator op = (ExpressionOperator) item;
            Preconditions.checkNotNull(op.getChildren());
            op.getChildren().stream()
                    .filter(ExpressionItem::enabled)
                    .forEach(child -> walkExpressionTree(child, targetFieldName, currentParents, foundPaths));
        } else {
            throw new RuntimeException(String.format("Unexpected instance type %s", item.getClass().getName()));
        }
    }
}
