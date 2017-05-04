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

package stroom.stats;

import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import stroom.dashboard.expression.FieldIndexMap;
import stroom.query.*;
import stroom.query.api.*;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.*;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.logging.LambdaLogger;
import stroom.util.shared.HasTerminate;

import javax.inject.Inject;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

//TODO everything about this class needs work, including its name
//TODO Does this need to be a singleton?
//@Singleton
public class HBaseClient implements Managed {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseClient.class);

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

    private static final SearchResponse EMPTY_SEARCH_RESPONSE = new SearchResponse(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            true);

    private final StatisticsService statisticsService;
    private final StatisticConfigurationService statisticConfigurationService;

    @Inject
    public HBaseClient(final StatisticsService statisticsService, final StatisticConfigurationService statisticConfigurationService) {
        this.statisticsService = statisticsService;
        this.statisticConfigurationService = statisticConfigurationService;
    }

    /**
     * Recursive method to populate the passed list with all enabled
     * {@link ExpressionTerm} nodes found in the tree.
     */
    public static List<ExpressionTerm> findAllTermNodes(final ExpressionItem node, final List<ExpressionTerm> termsFound) {
        Preconditions.checkNotNull(termsFound);
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
        return termsFound;
    }

    public void addStatistics(Statistics statistics) {
        Preconditions.checkNotNull(statistics);

        //TODO need a kafka producer to put these stats on a topic so they get processed via that route.
    }


    public SearchResponse query(SearchRequest searchRequest) {

        DocRef statisticStoreRef = searchRequest.getQuery().getDataSource();

        //TODO Need to consider how to handle an unknown docref
        return statisticConfigurationService.fetchStatisticConfigurationByUuid(statisticStoreRef.getUuid())
                .map(statisticConfiguration ->
                        performSearch(searchRequest, statisticConfiguration))
                .orElseGet(() -> new SearchResponse(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(
                                "Statistic configuration could not be found for uuid " + statisticStoreRef.getUuid()),
                        true)
                );
    }

    private SearchResponse performSearch(final SearchRequest searchRequest,
                                         final StatisticConfiguration statisticConfiguration) {

        // TODO: possibly the mapping from the componentId to the coprocessorsettings map is a bit odd.
        final CoprocessorSettingsMap coprocessorSettingsMap = CoprocessorSettingsMap.create(searchRequest);

        Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap = new HashMap<>();
        // TODO: Mapping to this is complicated! it'd be nice not to have to do this.
        final FieldIndexMap fieldIndexMap = new FieldIndexMap(true);

        // Compile all of the result component options to optimise pattern matching etc.
        if (coprocessorSettingsMap.getMap() != null) {
            for (final Map.Entry<CoprocessorSettingsMap.CoprocessorKey, CoprocessorSettings> entry : coprocessorSettingsMap.getMap().entrySet()) {
                final CoprocessorSettingsMap.CoprocessorKey coprocessorId = entry.getKey();
                final CoprocessorSettings coprocessorSettings = entry.getValue();

                // Create a parameter map.
                final Map<String, String> paramMap = Collections.emptyMap();
                if (searchRequest.getQuery().getParams() != null) {
                    for (final Param param : searchRequest.getQuery().getParams()) {
                        paramMap.put(param.getKey(), param.getValue());
                    }
                }

                final Coprocessor coprocessor = createCoprocessor(
                        coprocessorSettings, fieldIndexMap, paramMap, new HasTerminate() {
                            //TODO do something about this
                            @Override
                            public void terminate() {
                                System.out.println("terminating");
                            }

                            @Override
                            public boolean isTerminated() {
                                return false;
                            }
                        });

                if (coprocessor != null) {
                    coprocessorMap.put(coprocessorId, coprocessor);
                }
            }
        }

        List<String> requiredDynamicFields = getRequestedFields(statisticConfiguration, fieldIndexMap);

        //convert the generic query API SerachRequset object into a criteria object specific to
        //the way stats can be queried.
        SearchStatisticsCriteria criteria = buildCriteria(searchRequest, requiredDynamicFields, statisticConfiguration);

        StatisticDataSet statisticDataSet = statisticsService.searchStatisticsData(criteria, statisticConfiguration);

        if (!statisticDataSet.isEmpty()) {

            //TODO TableCoprocessor is doing a lot of work to pre-process and aggregate the datas

            for (StatisticDataPoint statisticDataPoint : statisticDataSet) {
                String[] dataArray = new String[fieldIndexMap.size()];

                //TODO should probably drive this off a new fieldIndexMap.getEntries() method or similar
                //then we only loop round fields we car about
                statisticConfiguration.getAllFieldNames().forEach(fieldName -> {
                    int posInDataArray = fieldIndexMap.get(fieldName);
                    //if the fieldIndexMap returns -1 the field has not been requested
                    if (posInDataArray != -1) {
                        dataArray[posInDataArray] = statisticDataPoint.getFieldValue(fieldName);
                    }
                });

                coprocessorMap.entrySet().forEach(coprocessor -> {
                    coprocessor.getValue().receive(dataArray);
                });
            }

            // TODO putting things into a payload and taking them out again is a waste of time in this case. We could use a queue instead and that'd be fine.
            //TODO: 'Payload' is a cluster specific name - what lucene ships back from a node.
            // Produce payloads for each coprocessor.
            Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap = null;
            if (coprocessorMap != null && coprocessorMap.size() > 0) {
                for (final Map.Entry<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> entry : coprocessorMap.entrySet()) {
                    final Payload payload = entry.getValue().createPayload();
                    if (payload != null) {
                        if (payloadMap == null) {
                            payloadMap = new HashMap<>();
                        }
                        payloadMap.put(entry.getKey(), payload);
                    }
                }
            }

            StatisticsStore store = new StatisticsStore();
            store.process(coprocessorSettingsMap);
            store.coprocessorMap(coprocessorMap);
            store.payloadMap(payloadMap);

            SearchResponseCreator searchResponseCreator = new SearchResponseCreator(store);
            SearchResponse searchResponse = searchResponseCreator.create(searchRequest);

            return searchResponse;
        } else {
            return EMPTY_SEARCH_RESPONSE;
        }
    }

    private List<String> getRequestedFields(final StatisticConfiguration statisticConfiguration,
                                            final FieldIndexMap fieldIndexMap) {

        List<String> requestedFields = new ArrayList<>();

        //TODO this is not ideal.  Need to expose the underlying map of FieldIndexMap so we can iterate over that
        //instead of iterating over all possible fields and seeing if they have been request by their presence
        //in the FieldIndexMap.  In reality the number of fields will never be more than 15 so
        //it is not a massive performance hit, just a bit grim.  Requires a change to the API to improve this.

        statisticConfiguration.getFieldNames().stream()
                .filter(staticField -> fieldIndexMap.get(staticField) != -1)
                .forEach(requestedFields::add);

        return requestedFields;
    }

    //TODO need an endpoint to kick off a purge for a list of docrefs

    public void purgeAllData(List<DocRef> docRefs) {
        //Quietly ignores docRefs that don't exist, may want to change this behaviour
        List<StatisticConfiguration> statisticConfigurations = docRefs.stream()
                .map(docRef -> statisticConfigurationService.fetchStatisticConfigurationByUuid(docRef.getUuid()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        statisticsService.purgeAllData(statisticConfigurations);
    }

    /**
     * Deletes ALL data from all granularities for all statistic configurations.
     * Use with EXTREME caution. Probably not for exposure to the web service
     */
    public void purgeAllData() {
        statisticsService.purgeAllData(statisticConfigurationService.fetchAll());
    }

    //TODO need an endpoint to kick off a retention period purge if we want to use cron jobs
    //to fire cluster level scheduled events


    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
        statisticsService.shutdown();
    }

    //TODO This lives in stroom and should have a copy here
    public static Coprocessor createCoprocessor(final CoprocessorSettings settings,
                                                final FieldIndexMap fieldIndexMap, final Map<String, String> paramMap, final HasTerminate taskMonitor) {
        if (settings instanceof TableCoprocessorSettings) {
            final TableCoprocessorSettings tableCoprocessorSettings = (TableCoprocessorSettings) settings;
            final TableCoprocessor tableCoprocessor = new TableCoprocessor(tableCoprocessorSettings,
                    fieldIndexMap, taskMonitor, paramMap);
            return tableCoprocessor;
        }
        return null;
    }

    /**
     * @param requiredDynamicFields A List of the dynamic fields (aka Tags) thare are required in the result set
     * @return A search criteria object specific to searching the stat store
     */
    static SearchStatisticsCriteria buildCriteria(final SearchRequest searchRequest,
                                                  final List<String> requiredDynamicFields,
                                                  final StatisticConfiguration statisticConfiguration) {

        LOGGER.trace(() -> String.format("buildCriteria called for statisticConfiguration %s", statisticConfiguration));

        Preconditions.checkNotNull(searchRequest);
        Preconditions.checkNotNull(requiredDynamicFields);
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

        optDateTimeTerm.ifPresent(HBaseClient::validateDateTerm);
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

        final SearchStatisticsCriteria.SearchStatisticsCriteriaBuilder criteriaBuilder = SearchStatisticsCriteria
                .builder(new Period(range.getFrom(), range.getTo()), statisticConfiguration.getName())
                .setFilterTermsTree(filterTermsTree)
                .setRequiredDynamicFields(requiredDynamicFields)
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
        Preconditions.checkArgument(ExpressionTerm.Condition.EQUALS.equals(precisionTerm.getCondition()),
                "Precision field only supports EQUALS as a condition");
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
            throw new UnsupportedOperationException(String.format("Field %s is only allowed to appear once in the expression tree", fieldName));
        } else if (foundPaths.size() == 1) {
            List<ExpressionItem> path = foundPaths.get(0);

            if (rootOperator.getOp().equals(expectedRootOp) && path.size() == expectedPathLength) {
                return Optional.of((ExpressionTerm) path.get(expectedPathLength - 1));
            } else {
                throw new UnsupportedOperationException(String.format("Field %s must appear as a direct child to the root operator which must be %s",
                        fieldName, expectedRootOp));
            }
        } else {
            return Optional.empty();
        }
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
                currentParents.add(item);
                List<ExpressionItem> path = new ArrayList<>(currentParents);
                foundPaths.add(path);
            }
        } else if (item instanceof ExpressionOperator) {
            ExpressionOperator op = (ExpressionOperator) item;
            currentParents.add(item);
            Preconditions.checkNotNull(op.getChildren());
            op.getChildren().stream()
                    .filter(ExpressionItem::enabled)
                    .forEach(child -> walkExpressionTree(child, targetFieldName, currentParents, foundPaths));
        } else {
            throw new RuntimeException(String.format("Unexpected instance type %s", item.getClass().getName()));
        }
    }

    private List<String> getQueryableFields(final StatisticConfiguration statisticConfiguration) {
        List<String> queryableFields = new ArrayList<>(statisticConfiguration.getFieldNames());
        queryableFields.addAll(SUPPORTED_QUERYABLE_STATIC_FIELDS);
        return queryableFields;
    }
}
