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
import javaslang.Tuple2;
import org.junit.Test;
import stroom.query.api.DocRef;
import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.Field;
import stroom.query.api.FieldBuilder;
import stroom.query.api.Query;
import stroom.query.api.QueryKey;
import stroom.query.api.ResultRequest;
import stroom.query.api.Row;
import stroom.query.api.SearchRequest;
import stroom.query.api.SearchResponse;
import stroom.query.api.TableResult;
import stroom.query.api.TableSettings;
import stroom.query.api.TableSettingsBuilder;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        return buildSearchRequestAllData(statisticConfiguration, interval, statisticConfiguration.getAllFieldNames());
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

    public static Map<String, String> convertRow(final Row row, final Map<String, Integer> fieldIndices) {

        return fieldIndices.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), row.getValues().get(entry.getValue())))
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    }

    public static List<Map<String, String>> getRowData(final SearchRequest searchRequest,
                                                       final SearchResponse searchResponse) {

        Map<String, Integer> fieldIndices = getFieldIndices(searchRequest);

        return ((TableResult) searchResponse.getResults().get(0)).getRows().stream()
                .map(row -> convertRow(row, fieldIndices))
                .collect(Collectors.toList());
    }

    public static ExpressionTerm buildIntervalTerm(final EventStoreTimeIntervalEnum interval) {
        return new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION,
                ExpressionTerm.Condition.EQUALS,
                interval.name().toLowerCase());
    }

    public static List<String> convertToFixedWidth(List<Map<String, String>> rowData, @Nullable Map<String, Class<?>> fieldTypes) {

        //TODO would be good to make this work from a SerachRequest/SearchResponse pair, then it can take the list
        //of fields from the table settings in the request, observing that field order.
        //Also woudl be nice to be able to do things like
        // .configureField(new FieldConfigBuilder("myField").leftJustify().convert(conversionFunc).build())
        //Also may be nice to be able to configure ascii table vs csv vs tab delim and header/noHeder etc.


        //assume all rows have same fields so just use first one
        if (rowData == null || rowData.isEmpty()) {
            return Collections.emptyList();
        } else {
            //get the widths of the field headings
            List<String> fieldNames = rowData.get(0).keySet().stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());

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
                    .map(rowMap -> maxFieldWidths.entrySet().stream()
                            .map(entry -> Strings.padStart(rowMap.get(entry.getKey()), entry.getValue() + 1, ' '))
                            .map(str -> str + " ")
                            .collect(Collectors.joining("|")))
                    .collect(Collectors.toList());

            String headerString = maxFieldWidths.entrySet().stream()
                            .map(entry -> Strings.padStart(entry.getKey(), entry.getValue() + 1, ' '))
                            .map(str -> str + " ")
                            .collect(Collectors.joining("|"));

            List<String> headerAndValueStrings = new ArrayList<>();
            headerAndValueStrings.add(headerString);
            headerAndValueStrings.add(createHorizontalLine(headerString.length(), '-'));
            headerAndValueStrings.addAll(valueStrings);
            return headerAndValueStrings;
        }

    }

    private static String createHorizontalLine(int length, char lineChar) {
        return Strings.repeat(String.valueOf(lineChar), length);
    }

    @Test
    public void testConvertToFixedWidth() {

        Map<String, String> row1 = new HashMap<>();
        row1.put("heading1", "123");
        row1.put("h2", "45678");

        Map<String, String> row2 = new HashMap<>();
        row2.put("heading1", "2345");
        row2.put("h2", "9");

        List<Map<String, String>> rowData = Arrays.asList(row1, row2);

        convertToFixedWidth(rowData, null).forEach(System.out::println);
    }


}
