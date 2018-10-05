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

package stroom.stats.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import stroom.dashboard.expression.v1.Val;
import stroom.dashboard.expression.v1.ValLong;
import stroom.dashboard.expression.v1.ValNull;
import stroom.dashboard.expression.v1.ValString;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class BasicStatisticDataPoint implements StatisticDataPoint {

    private static final Map<String, Function<BasicStatisticDataPoint, Val>> FIELD_VALUE_FUNCTION_MAP;

    static {
        FIELD_VALUE_FUNCTION_MAP = ImmutableMap.<String, Function<BasicStatisticDataPoint, Val>>builder()
                .put(StatisticConfiguration.FIELD_NAME_DATE_TIME, dataPoint ->
                        ValLong.create(dataPoint.getTimeMs()))
                .put(StatisticConfiguration.FIELD_NAME_STATISTIC, dataPoint ->
                        ValString.create(dataPoint.statisticConfiguration.getName()))
                .put(StatisticConfiguration.FIELD_NAME_UUID, dataPoint ->
                        ValString.create(dataPoint.statisticConfiguration.getUuid()))
                .put(StatisticConfiguration.FIELD_NAME_PRECISION, dataPoint ->
                        ValString.create(dataPoint.precision.longName()))
                .put(StatisticConfiguration.FIELD_NAME_PRECISION_MS, dataPoint ->
                        ValLong.create(dataPoint.precision.columnInterval()))
                .build();
    }

    private final StatisticConfiguration statisticConfiguration;
    private final EventStoreTimeIntervalEnum precision;
    private final long timeMs;
    private final List<StatisticTag> tags;
    private final Map<String, String> tagToValueMap;

    BasicStatisticDataPoint(
            final StatisticConfiguration statisticConfiguration,
            final EventStoreTimeIntervalEnum precision,
            final long timeMs,
            final List<StatisticTag> tags) {

        this.statisticConfiguration = statisticConfiguration;
        this.precision = precision;
        Preconditions.checkNotNull(statisticConfiguration);
        Preconditions.checkNotNull(precision);
        Preconditions.checkArgument(timeMs >= 0);
        Preconditions.checkNotNull(tags);

        this.timeMs = timeMs;
        this.tags = Collections.unmodifiableList(tags);

        final Map<String, String> tempMap = new HashMap<>();
        tags.forEach(tag -> tempMap.put(tag.getTag(), tag.getValue()));
        this.tagToValueMap = Collections.unmodifiableMap(tempMap);
    }

    @Override
    public StatisticConfiguration getStatisticConfiguration() {
        return statisticConfiguration;
    }

    @Override
    public EventStoreTimeIntervalEnum getTimeInterval() {
        return precision;
    }

    @Override
    public long getTimeMs() {
        return timeMs;
    }

    @Override
    public List<StatisticTag> getTags() {
        return tags;
    }

    @Override
    public Map<String, String> getTagsAsMap() {
        Map<String, String> map = new HashMap<>();
        for (StatisticTag tag : tags) {
            map.put(tag.getTag(), tag.getValue());
        }
        return ImmutableMap.copyOf(tagToValueMap);
    }

    @Override
    public StatisticType getStatisticType() {
        return statisticConfiguration.getStatisticType();
    }

    @Override
    public Val getFieldValue(final String fieldName) {
        Function<BasicStatisticDataPoint, Val> fieldValueFunction = FIELD_VALUE_FUNCTION_MAP.get(fieldName);

        if (fieldValueFunction == null) {
            //either it is a tag field or we don't know about this field
            String tagValue = tagToValueMap.get(fieldName);
            return tagValue != null ? ValString.create(tagValue) : ValNull.INSTANCE;
        } else {
            return fieldValueFunction.apply(this);
        }
    }

    @Override
    public String toString() {
        return "BasicStatisticDataPoint{" +
                "statisticConfiguration=" + statisticConfiguration +
                ", precision=" + precision +
                ", timeMs=" + timeMs +
                ", tagToValueMap=" + tagToValueMap +
                '}';
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BasicStatisticDataPoint that = (BasicStatisticDataPoint) o;

        if (timeMs != that.timeMs) return false;
        if (!statisticConfiguration.equals(that.statisticConfiguration)) return false;
        if (precision != that.precision) return false;
        return tags.equals(that.tags);
    }

    @Override
    public int hashCode() {
        int result = statisticConfiguration.hashCode();
        result = 31 * result + precision.hashCode();
        result = 31 * result + (int) (timeMs ^ (timeMs >>> 32));
        result = 31 * result + tags.hashCode();
        return result;
    }
}
