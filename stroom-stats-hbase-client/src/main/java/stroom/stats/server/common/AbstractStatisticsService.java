

/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.server.common;

import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.Query;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
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
import stroom.stats.util.DateUtil;
import stroom.stats.util.logging.LambdaLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractStatisticsService implements StatisticsService {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(AbstractStatisticsService.class);

    private static final List<ExpressionTerm.Condition> SUPPORTED_DATE_CONDITIONS = Arrays
            .asList(new ExpressionTerm.Condition[]{ExpressionTerm.Condition.BETWEEN});

    private final StatisticConfigurationValidator statisticConfigurationValidator;
    private final StatisticConfigurationService statisticConfigurationService;
    private final StroomPropertyService propertyService;

    public AbstractStatisticsService(final StatisticConfigurationValidator statisticConfigurationValidator,
                                     final StatisticConfigurationService statisticConfigurationService,
                                     final StroomPropertyService propertyService) {
        this.statisticConfigurationValidator = statisticConfigurationValidator;
        this.statisticConfigurationService = statisticConfigurationService;
        this.propertyService = propertyService;
    }


    @Override
    public boolean putEvents(final List<StatisticEvent> statisticEvents) {

        statisticEvents.stream()
                .collect(Collectors.groupingBy(StatisticEvent::getName))
                .forEach((statisticName, groupedStatisticEvents) -> {
                    StatisticConfiguration statisticConfiguration = getStatisticConfiguration(statisticName);
                    putEvents(statisticConfiguration, groupedStatisticEvents);
                });
        return true;
    }


//    protected boolean validateStatisticConfiguration(final StatisticEvent statisticEvent,
//                                                  final StatisticConfiguration statisticConfiguration) {
//        if (statisticConfigurationValidator != null) {
//            return statisticConfigurationValidator.validateStatisticConfiguration(statisticEvent.getName(),
//                    statisticEvent.getType(), statisticConfiguration);
//        } else {
//            // no validator has been supplied so return true
//            return true;
//        }
//    }

    protected static FindEventCriteria buildCriteria(final Query query, final StatisticConfiguration dataSource) {
        LOGGER.trace(() -> String.format("buildCriteria called for statistic %s", dataSource.getName()));

        // object looks a bit like this
        // AND
        // Date Time between 2014-10-22T23:00:00.000Z,2014-10-23T23:00:00.000Z

        final ExpressionOperator topLevelExpressionOperator = query.getExpression();

        if (topLevelExpressionOperator == null || topLevelExpressionOperator.getOp().getDisplayValue() == null) {
            throw new IllegalArgumentException(
                    "The top level operator for the query must be one of [" + ExpressionOperator.Op.values() + "]");
        }

        final List<ExpressionItem> childExpressions = topLevelExpressionOperator.getChildren();
        int validDateTermsFound = 0;
        int dateTermsFound = 0;

        // Identify the date term in the search criteria. Currently we must have
        // a exactly one BETWEEN operator on the
        // datetime
        // field to be able to search. This is because of the way the search in
        // hbase is done, ie. by start/stop row
        // key.
        // It may be possible to expand the capability to make multiple searches
        // but that is currently not in place
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
                            + "' field with one of the following condtitions [" + SUPPORTED_DATE_CONDITIONS.toString()
                            + "].  Please amend the query");
        }

        // ensure the value field is not used in the query terms
        query.getExpression().getChildren().forEach(expressionItem -> {
            if(expressionItem instanceof ExpressionTerm
                    && ((ExpressionTerm)expressionItem).getValue().contains(StatisticConfiguration.FIELD_NAME_VALUE)){
                throw new UnsupportedOperationException("Search queries containing the field '"
                        + StatisticConfiguration.FIELD_NAME_VALUE + "' are not supported.  Please remove it from the query");
            }
        });

        // if we have got here then we have a single BETWEEN date term, so parse
        // it.
        final Range<Long> range = extractRange(dateTerm);

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
            if (dataSource.getRollUpType().equals(StatisticRollUpType.NONE)) {
                throw new UnsupportedOperationException(
                        "Query contains rolled up terms but the Statistic Data Source does not support any roll-ups");
            } else if (dataSource.getRollUpType().equals(StatisticRollUpType.CUSTOM)) {
                if (!dataSource.isRollUpCombinationSupported(rolledUpFieldNames)) {
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

        final FindEventCriteria criteria = FindEventCriteria.builder(new Period(range.getFrom(), range.getTo()), dataSource.getName())
                .setFilterTermsTree(filterTermsTree)
                .setRolledUpFieldNames(rolledUpFieldNames)
                .build();

        LOGGER.info(String.format("Searching statistics store with criteria: {}", criteria));
        return criteria;
    }

    // TODO could go futher up the chain so is store agnostic
    public static RolledUpStatisticEvent generateTagRollUps(final StatisticEvent event,
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

    private static Range<Long> extractRange(final ExpressionTerm dateTerm) {
        long rangeFrom = 0;
        long rangeTo = Long.MAX_VALUE;

        final String[] dateArr = dateTerm.getValue().split(",");

        if (dateArr.length != 2) {
            throw new RuntimeException("DateTime term is not a valid format, term: " + dateTerm.toString());
        }

        rangeFrom = DateUtil.parseNormalDateTimeString(dateArr[0]);
        // add one to make it exclusive
        rangeTo = DateUtil.parseNormalDateTimeString(dateArr[1]) + 1;

        final Range<Long> range = new Range<>(rangeFrom, rangeTo);

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

    protected StatisticConfiguration getStatisticConfiguration(final String statisticName) {
        return statisticConfigurationService.fetchStatisticConfigurationByName(statisticName)
                .orElseThrow(() -> new RuntimeException("No statistic configuration exists with name: " + statisticName));
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

    /**
     * Template method, should be overridden by a sub-class if it needs to black
     * list certain index fields
     *
     * @return
     */
    protected Set<String> getIndexFieldBlackList() {
        return Collections.emptySet();
    }

    public List<Set<Integer>> getFieldPositionsForBitMasks(final List<Short> maskValues) {
        if (maskValues != null) {
            final List<Set<Integer>> tagPosPermsList = new ArrayList<>();

            for (final Short maskValue : maskValues) {
                tagPosPermsList.add(RollUpBitMask.fromShort(maskValue).getTagPositions());
            }
            return tagPosPermsList;
        } else {
            return Collections.emptyList();
        }
    }

    public abstract StatisticDataSet searchStatisticsData(final Query query, final StatisticConfiguration statisticConfiguration);
}
