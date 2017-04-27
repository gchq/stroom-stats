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

package stroom.stats.test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javaslang.Tuple2;
import joptsimple.internal.Rows;
import org.junit.Test;
import stroom.query.api.*;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryApiHelper {

    private static final Map<Class<?>, Function<String, Object>> conversionMap = new HashMap<>();

    static {
        conversionMap.put(String.class, str -> str);
        conversionMap.put(Long.class, Long::valueOf);
        conversionMap.put(Double.class, Double::valueOf);
        conversionMap.put(Instant.class, str -> Instant.ofEpochMilli(Long.valueOf(str)));
        conversionMap.put(ZonedDateTime.class, str ->
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(str)), ZoneOffset.UTC));
    }

    private static final char TABLE_COLUMN_DELIMITER = '|';
    private static final char TABLE_HEADER_DELIMITER = '-';

    public static SearchRequest buildSearchRequestAllData(final StatisticConfiguration statisticConfiguration,
                                                          @Nullable final EventStoreTimeIntervalEnum interval,
                                                          List<String> requestedFieldNames) {

        //Add the interval to the predicates if we have one to let us specify the store rather than have it guess
        ExpressionItem[] childExpressionItems;
        if (interval == null) {
            childExpressionItems = new ExpressionItem[0];
        } else {
            childExpressionItems = new ExpressionItem[]{buildIntervalTerm(interval)};
        }

        ExpressionOperator expressionOperator = new ExpressionOperator(
                true,
                ExpressionOperator.Op.AND, childExpressionItems);

        SearchRequest searchRequest = buildSearchRequest(
                statisticConfiguration,
                expressionOperator,
                requestedFieldNames);

        return searchRequest;
    }

    /**
     * Create a search request with no predicates (unless an interval is supplied) and with all fields
     * in the returned data set
     */
    public static SearchRequest buildSearchRequestAllDataAllFields(final StatisticConfiguration statisticConfiguration,
                                                                   @Nullable final EventStoreTimeIntervalEnum interval) {
        List<String> fields = new ArrayList<>();
        fields.add(StatisticConfiguration.FIELD_NAME_STATISTIC);
        fields.add(StatisticConfiguration.FIELD_NAME_DATE_TIME);

        //add the dynamic fields
        fields.addAll(statisticConfiguration.getFieldNames());

        switch (statisticConfiguration.getStatisticType()) {
            case COUNT:
                fields.add(StatisticConfiguration.FIELD_NAME_COUNT);
                break;
            case VALUE:
                fields.add(StatisticConfiguration.FIELD_NAME_VALUE);
                fields.add(StatisticConfiguration.FIELD_NAME_MIN_VALUE);
                fields.add(StatisticConfiguration.FIELD_NAME_MAX_VALUE);
                fields.add(StatisticConfiguration.FIELD_NAME_COUNT);
                break;
            default:
                throw new RuntimeException("Unexpected stat type " + statisticConfiguration.getStatisticType());
        }

        fields.add(StatisticConfiguration.FIELD_NAME_PRECISION);

        return buildSearchRequestAllData(statisticConfiguration, interval, fields);
    }

    public static SearchRequest buildSearchRequest(final StatisticConfiguration statisticConfiguration,
                                                   final ExpressionOperator rootOperator,
                                                   final List<String> fieldNames) {
        Query query = new Query(
                new DocRef(
                        StatisticConfiguration.ENTITY_TYPE,
                        statisticConfiguration.getUuid(),
                        statisticConfiguration.getName()),
                rootOperator);

        SearchRequest searchRequest = wrapQuery(query, fieldNames);

        return searchRequest;
    }

    public static SearchRequest wrapQuery(Query query, List<String> fieldNames) {

        //build the fields for the search response table settings
        List<Field> fields = fieldNames.stream()
                .map(String::toLowerCase)
                .map(field -> new FieldBuilder().name(field).expression("${" + field + "}").build())
                .collect(Collectors.toList());

        TableSettings tableSettings = new TableSettingsBuilder()
                .fields(fields)
                .build();

        ResultRequest resultRequest = new ResultRequest("mainResult", tableSettings);

        return new SearchRequest(
                new QueryKey(UUID.randomUUID().toString()),
                query,
                Collections.singletonList(resultRequest),
                ZoneOffset.UTC.getId(),
                false);
    }

    /**
     * Get all values for a named field, converted into the chosen type
     */
    public static <T> List<T> getTypedFieldValues(final SearchRequest searchRequest,
                                                  final SearchResponse searchResponse,
                                                  final String fieldName,
                                                  final Class<T> valueType) {

        //assume only one result request and one tablesSetting
        int fieldIndex = searchRequest.getResultRequests().get(0).getMappings().get(0).getFields().stream()
                .map(field -> field.getName().toLowerCase())
                .collect(Collectors.toList())
                .indexOf(fieldName.toLowerCase());

        Function<String, T> conversionFunc = str -> {
            Object val = conversionMap.get(valueType).apply(str);
            try {
                return (T) val;
            } catch (ClassCastException e) {
                throw new RuntimeException(String.format("Unable to cast field %s to type %s", fieldName, valueType.getName()), e);
            }
        };

        return ((TableResult) searchResponse.getResults().get(0)).getRows().stream()
                .map(row -> row.getValues().get(fieldIndex))
                .map(conversionFunc)
                .collect(Collectors.toList());
    }

    public static Map<String, Integer> getFieldIndices(final SearchRequest searchRequest) {
        Map<String, Integer> fieldIndices = new HashMap<>();
        List<String> fieldNames = searchRequest.getResultRequests().get(0).getMappings().get(0).getFields().stream()
                .map(field -> field.getName().toLowerCase())
                .collect(Collectors.toList());

        int index = 0;
        for (String fieldName : fieldNames) {
            fieldIndices.put(fieldName, index++);
        }
        return fieldIndices;
    }

    public static Map<String, Integer> getFieldIndices(final List<Field> fields) {
        Map<String, Integer> fieldIndices = new HashMap<>();
        List<String> fieldNames = fields.stream()
                .map(Field::getName)
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        int index = 0;
        for (String fieldName : fieldNames) {
            fieldIndices.put(fieldName, index++);
        }
        return fieldIndices;
    }

    public static Map<String, String> convertRow(final Row row,
                                                 final Map<String, Integer> fieldIndices) {

        return fieldIndices.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), row.getValues().get(entry.getValue())))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    public static Map<String, String> convertValues(final List<Object> values,
                                                    final Map<String, Integer> fieldIndices) {

        final Function<Object, String> valueToStrMapper = (obj) -> {
            if (obj instanceof String) {
                return (String) obj;
            } else {
                return obj.toString();
            }
        };
        return fieldIndices.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), values.get(entry.getValue())))
                .map(tuple2 -> tuple2.map2(valueToStrMapper))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    public static int getRowCount(final SearchResponse searchResponse) {

        List<Result> results = searchResponse.getResults();

        if (results == null || results.isEmpty()) {
            return 0;
        }

        return ((TableResult) results.get(0)).getRows().size();
    }

    public static List<Map<String, String>> getRowData(final SearchRequest searchRequest,
                                                       final SearchResponse searchResponse) {

        Map<String, Integer> fieldIndices = getFieldIndices(searchRequest);

        List<Result> results = searchResponse.getResults();

        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        }

        return ((TableResult) results.get(0)).getRows().stream()
                .map(row -> convertRow(row, fieldIndices))
                .collect(Collectors.toList());
    }

    public static ExpressionTerm buildIntervalTerm(final EventStoreTimeIntervalEnum interval) {
        return new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION,
                ExpressionTerm.Condition.EQUALS,
                interval.name().toLowerCase());
    }

    /**
     * Build an ascii table from a searchRequest/Response
     * @param searchRequest
     * @param searchResponse
     * @param fieldTypes An optional map of field names to their types, else string will be assumed
     * @return A list of strings representing the ascii table, one per row, including a header row and
     * separator between header and data
     */
    public static List<String> convertToFixedWidth(final SearchRequest searchRequest,
                                                   final SearchResponse searchResponse,
                                                   @Nullable Map<String, Class<?>> fieldTypes,
                                                   @Nullable Integer maxRows) {

        //TODO would be good to make this work from a SerachRequest/SearchResponse pair, then it can take the list
        //of fields from the table settings in the request, observing that field order.
        //Also woudl be nice to be able to do things like
        // .configureField(new FieldConfigBuilder("myField").leftJustify().convert(conversionFunc).build())
        //Also may be nice to be able to configure ascii table vs csv vs tab delim and header/noHeder etc.

        List<Result> results = searchResponse.getResults() != null
                ? searchResponse.getResults()
                : Collections.emptyList();

        Map<String, Integer> fieldIndices = getFieldIndices(searchRequest);

        long rowLimit = maxRows != null ? maxRows : Long.MAX_VALUE;

        List<Map<String, String>> rowData = ((TableResult) results.get(0)).getRows().stream()
                .limit(rowLimit)
                .map(row -> convertRow(row, fieldIndices))
                .collect(Collectors.toList());

        //assume all rows have same fields so just use first one
        if (rowData == null || rowData.isEmpty()) {
            return Collections.emptyList();
        } else {
            //Get the field names in index order
            List<String> fieldNames = fieldIndices.entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            //get the widths of the field headings
            Map<String, Integer> maxFieldWidths = new HashMap<>();

            List<Map<String, String>> formattedRowData;

            //if we have been given typed for any fields then do then convert those values
            if (fieldTypes == null || fieldTypes.isEmpty()) {
                formattedRowData = rowData;
            } else {
                formattedRowData = rowData.stream()
                        .map(rowMap -> {
                            Map<String, String> newRowMap = new HashMap<>();
                            fieldNames.forEach(fieldName -> {
                                Class<?> type = fieldTypes.get(fieldName);
                                if (type != null) {
                                    String newValue = conversionMap.get(type).apply(rowMap.get(fieldName)).toString();
                                    newRowMap.put(fieldName, newValue);
                                } else {
                                    //no explicit type so take the value as is
                                    newRowMap.put(fieldName, rowMap.get(fieldName));
                                }
                            });
                            return newRowMap;
                        })
                        .collect(Collectors.toList());
            }

            fieldNames.forEach(key -> maxFieldWidths.put(key, key.length()));

            //now find the max width for each value (and its field heading)
            formattedRowData.stream()
                    .flatMap(rowMap -> rowMap.entrySet().stream())
                    .forEach(entry ->
                            maxFieldWidths.merge(entry.getKey(), entry.getValue().length(), Math::max));

            //now construct the row strings
            List<String> valueStrings = formattedRowData.stream()
                    .map(rowMap -> fieldNames.stream()
                            .map(fieldName -> formatCell(rowMap.get(fieldName), maxFieldWidths.get(fieldName)))
                            .collect(Collectors.joining(String.valueOf(TABLE_COLUMN_DELIMITER))))
                    .collect(Collectors.toList());

            String headerString = fieldNames.stream()
                    .map(fieldName -> formatCell(fieldName, maxFieldWidths.get(fieldName)))
                    .collect(Collectors.joining(String.valueOf(TABLE_COLUMN_DELIMITER)));

            List<String> headerAndValueStrings = new ArrayList<>();
            headerAndValueStrings.add(headerString);
            headerAndValueStrings.add(createHorizontalLine(headerString.length(), TABLE_HEADER_DELIMITER));
            headerAndValueStrings.addAll(valueStrings);

            if (maxRows != null) {
                headerAndValueStrings.add(String.format("\n...TRUNCATED TO %s ROWS...", maxRows));
            }

            return headerAndValueStrings;
        }
    }

    public static List<String> convertToFixedWidth(final FlatResult flatResult,
                                                   @Nullable Map<String, Class<?>> fieldTypes,
                                                   @Nullable Integer maxRows) {


        Map<String, Integer> fieldIndices = getFieldIndices(flatResult.getStructure());

        long rowLimit = maxRows != null ? maxRows : Long.MAX_VALUE;

        List<Map<String, String>> rowData = flatResult.getValues().stream()
                .limit(rowLimit)
                .map(values -> convertValues(values, fieldIndices))
                .collect(Collectors.toList());

        //assume all rows have same fields so just use first one
        if (rowData == null || rowData.isEmpty()) {
            return Collections.emptyList();
        } else {
            //Get the field names in index order
            List<String> fieldNames = fieldIndices.entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            //get the widths of the field headings
            Map<String, Integer> maxFieldWidths = new HashMap<>();

            List<Map<String, String>> formattedRowData;

            //if we have been given typed for any fields then do then convert those values
            if (fieldTypes == null || fieldTypes.isEmpty()) {
                formattedRowData = rowData;
            } else {
                formattedRowData = rowData.stream()
                        .map(rowMap -> {
                            Map<String, String> newRowMap = new HashMap<>();
                            fieldNames.forEach(fieldName -> {
                                Class<?> type = fieldTypes.get(fieldName);
                                if (type != null) {
                                    String newValue = conversionMap.get(type).apply(rowMap.get(fieldName)).toString();
                                    newRowMap.put(fieldName, newValue);
                                } else {
                                    //no explicit type so take the value as is
                                    newRowMap.put(fieldName, rowMap.get(fieldName));
                                }
                            });
                            return newRowMap;
                        })
                        .collect(Collectors.toList());
            }

            fieldNames.forEach(key -> maxFieldWidths.put(key, key.length()));

            //now find the max width for each value (and its field heading)
            formattedRowData.stream()
                    .flatMap(rowMap -> rowMap.entrySet().stream())
                    .forEach(entry ->
                            maxFieldWidths.merge(entry.getKey(), entry.getValue().length(), Math::max));

            //now construct the row strings
            List<String> valueStrings = formattedRowData.stream()
                    .map(rowMap -> fieldNames.stream()
                            .map(fieldName -> formatCell(rowMap.get(fieldName), maxFieldWidths.get(fieldName)))
                            .collect(Collectors.joining(String.valueOf(TABLE_COLUMN_DELIMITER))))
                    .collect(Collectors.toList());

            String headerString = fieldNames.stream()
                    .map(fieldName -> formatCell(fieldName, maxFieldWidths.get(fieldName)))
                    .collect(Collectors.joining(String.valueOf(TABLE_COLUMN_DELIMITER)));

            List<String> headerAndValueStrings = new ArrayList<>();
            headerAndValueStrings.add(headerString);
            headerAndValueStrings.add(createHorizontalLine(headerString.length(), TABLE_HEADER_DELIMITER));
            headerAndValueStrings.addAll(valueStrings);

            if (maxRows != null) {
                headerAndValueStrings.add(String.format("\n...TRUNCATED TO %s ROWS...", maxRows));
            }

            return headerAndValueStrings;
        }
    }
    private static String formatCell(String value, int maxWidth) {
        return Strings.padStart(value, maxWidth + 1, ' ') + " ";
    }


    private static String createHorizontalLine(int length, char lineChar) {
        return Strings.repeat(String.valueOf(lineChar), length);
    }


    @Test
    public void testConvertToFixedWidth() {

        Query query = new Query(
                new DocRef(
                        StatisticConfiguration.ENTITY_TYPE,
                        UUID.randomUUID().toString(),
                        "myStatName"),
                null);

        SearchRequest searchRequest = wrapQuery(query, Arrays.asList("heading1", "h2"));

        List<Row> rows = ImmutableList.of(
                new Row("GroupKey", ImmutableList.of("123", "45678"), 0),
        new Row("GroupKey", ImmutableList.of("2345", "9"), 0)
        );

        List<Result> results = Collections.singletonList(
                new TableResult("componentId", rows, null, null, null));

        SearchResponse searchResponse = new SearchResponse(null, results, null, null);

        convertToFixedWidth(searchRequest, searchResponse, null, null)
                .forEach(System.out::println);
    }


}
