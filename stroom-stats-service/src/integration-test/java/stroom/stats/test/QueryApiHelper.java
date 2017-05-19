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
import javaslang.Tuple2;
import org.junit.Test;
import stroom.query.api.v1.DocRef;
import stroom.query.api.v1.ExpressionItem;
import stroom.query.api.v1.ExpressionOperator;
import stroom.query.api.v1.ExpressionTerm;
import stroom.query.api.v1.Field;
import stroom.query.api.v1.FieldBuilder;
import stroom.query.api.v1.FlatResult;
import stroom.query.api.v1.Query;
import stroom.query.api.v1.QueryKey;
import stroom.query.api.v1.Result;
import stroom.query.api.v1.ResultRequest;
import stroom.query.api.v1.Row;
import stroom.query.api.v1.SearchRequest;
import stroom.query.api.v1.SearchResponse;
import stroom.query.api.v1.TableSettings;
import stroom.query.api.v1.TableSettingsBuilder;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

    public static List<String> getStringFieldValues(final FlatResult flatResult,
                                                    final String fieldName) {
        return getTypedFieldValues(flatResult, fieldName, String.class);
    }

    /**
     * Get all values for a named field, converted into the chosen type
     */
    public static <T> List<T> getTypedFieldValues(final FlatResult flatResult,
                                                  final String fieldName,
                                                  final Class<T> valueType) {

        if (flatResult == null || flatResult.getValues() == null || flatResult.getValues().isEmpty()) {
            return Collections.emptyList();
        }

        int fieldIndex = getFieldIndex(flatResult.getStructure(), fieldName);
        if (fieldIndex == -1) {
            throw new RuntimeException(String.format("Field %s does not exist in the FlatResult, possible fields: %s",
                    fieldName, flatResult.getStructure().stream().map(Field::getName).collect(Collectors.joining(","))));
        }

        Function<String, T> conversionFunc = str -> {
            Object val = conversionMap.get(valueType).apply(str);
            try {
                return (T) val;
            } catch (ClassCastException e) {
                throw new RuntimeException(String.format("Unable to cast field %s to type %s", fieldName, valueType.getName()), e);
            }
        };

        //
        return flatResult.getValues().stream()
                .map(values -> values.get(fieldIndex))
                .map(obj -> {
                    if (obj.getClass().equals(valueType)) {
                        return (T) obj;
                    } else {
                        return conversionFunc.apply(convertValueToStr(obj));
                    }
                })
                .collect(Collectors.toList());
    }

    public static Map<String, Integer> getFieldIndices(final SearchRequest searchRequest) {
        Map<String, Integer> fieldIndices = new HashMap<>();
        List<String> fieldNames = searchRequest.getResultRequests().get(0).getMappings().get(0).getFields().stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());

        int index = 0;
        for (String fieldName : fieldNames) {
            fieldIndices.put(fieldName, index++);
        }
        return fieldIndices;
    }

    public static Map<String, Integer> getFieldIndices(final List<Field> fields) {
        return getFieldIndices(fields, false);
    }

    public static Map<String, Integer> getFieldIndices(final List<Field> fields, boolean includeInternalFields) {
        Map<String, Integer> fieldIndices = new HashMap<>();
        List<String> fieldNames = fields.stream()
                .map(Field::getName)
                .collect(Collectors.toList());

        int index = 0;
        for (String fieldName : fieldNames) {

            if (!fieldName.startsWith(":") || includeInternalFields) {
                fieldIndices.put(fieldName, index);
            }
            index++;
        }
        return fieldIndices;
    }

    public static int getFieldIndex(final List<Field> fields, final String fieldName) {
        int index = 0;
        for (Field field : fields) {
            if (field.getName().equals(fieldName)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    public static Map<String, String> convertRow(final Row row,
                                                 final Map<String, Integer> fieldIndices) {

        return fieldIndices.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), row.getValues().get(entry.getValue())))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    public static String convertValueToStr(final Object obj) {
        if (obj == null) {
            return "";
        } else if (obj instanceof String) {
            return (String) obj;
        } else {
            return obj.toString();
        }
    }

    public static Map<String, String> convertValuesToStrings(final List<Object> values,
                                                             final Map<String, Integer> fieldIndices) {

        return fieldIndices.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), values.get(entry.getValue())))
                .map(tuple2 -> tuple2.map2(QueryApiHelper::convertValueToStr))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    public static List<Map<String, String>> getRowData(final FlatResult flatResult) {

        Map<String, Integer> fieldIndices = getFieldIndices(flatResult.getStructure());

        List<List<Object>> results = flatResult.getValues();

        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        }

        return results.stream()
                .map(row -> convertValuesToStrings(row, fieldIndices))
                .collect(Collectors.toList());
    }

    public static Optional<FlatResult> getFlatResult(final SearchResponse searchResponse) {
        return getFlatResult(searchResponse, 0);
    }

    public static Optional<FlatResult> getFlatResult(final SearchResponse searchResponse, final int index) {
        if (searchResponse == null || searchResponse.getResults() == null || searchResponse.getResults().isEmpty()) {
            return Optional.empty();
        }
        List<Result> results = searchResponse.getResults();

        if (index >= results.size()) {
            throw new IndexOutOfBoundsException(String.format("Index %s is not valid for results of size %s", index, results.size()));
        }

        return Optional.of((FlatResult) results.get(index));
    }

    public static int getRowCount(final SearchResponse searchResponse) {
        return getRowCount(searchResponse, 0);
    }

    public static int getRowCount(final SearchResponse searchResponse, final int index) {
        return getFlatResult(searchResponse, index)
                .map(flatResult -> flatResult.getValues().size())
                .orElse(0);
    }

    public static ExpressionTerm buildIntervalTerm(final EventStoreTimeIntervalEnum interval) {
        return new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION,
                ExpressionTerm.Condition.EQUALS,
                interval.name().toLowerCase());
    }


    public static List<String> convertToFixedWidth(final FlatResult flatResult,
                                                   @Nullable Map<String, Class<?>> fieldTypes,
                                                   @Nullable Integer maxRows) {

        if (flatResult == null) {
            return Collections.emptyList();
        }

        Map<String, Integer> fieldIndices = getFieldIndices(flatResult.getStructure());

        long rowLimit = maxRows != null ? maxRows : Long.MAX_VALUE;
        boolean wasTruncated = flatResult.getValues() != null && rowLimit < flatResult.getValues().size();

        List<Map<String, String>> rowData = flatResult.getValues().stream()
                .limit(rowLimit)
                .map(values -> convertValuesToStrings(values, fieldIndices))
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

            if (wasTruncated) {
                headerAndValueStrings.add(String.format("...TRUNCATED TO %s ROWS...", rowLimit));
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

        List<Field> fields = Arrays.asList(
                new Field("heading1", "", null, null, null, null),
                new Field("h2", "", null, null, null, null));

        List<List<Object>> values = ImmutableList.of(
                ImmutableList.of("123", "45678"),
                ImmutableList.of("2345", "9"));


        FlatResult flatResult = new FlatResult("ComponentId", fields, values, 0L, null);

        convertToFixedWidth(flatResult, null, null)
                .forEach(System.out::println);
    }


}
