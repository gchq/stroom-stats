

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
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


/**
 * Value object to hold a statistic data point for a VALUE statistic as
 * retrieved from a statistic store.
 * This represents an aggregated value of 1-many statistic events
 */
public class ValueStatisticDataPoint implements StatisticDataPoint {
    private static final StatisticType STATISTIC_TYPE = StatisticType.VALUE;

    private final BasicStatisticDataPoint delegate;
    private final long count;
    private final double value;
    private final double minValue;
    private final double maxValue;

    private final Map<String, Supplier<String>> fieldValueSupplierMap;


    /**
     * Constructor for a value type statistic data point
     *
     * @param timeMs
     *            The timestamp of the aggregated data point
     * @param tags
     *            The list of tav/value pairs that qualify the data point
     * @param value
     *            The mean value of the data point in this time period
     * @param count
     *            The count of the number of statistic events that have happened
     *            in this period
     * @param minValue
     *            The min value in this time period
     * @param maxValue
     *            The max value in this time period
     * @return A populated {@link ValueStatisticDataPoint} instance
     */
    public ValueStatisticDataPoint(final long timeMs, final long precisionMs, final List<StatisticTag> tags,
                                   final long count, final double value, final double minValue, final double maxValue) {
        this.delegate = new BasicStatisticDataPoint(timeMs, precisionMs, tags);
        this.count = count;
        this.value = value;
        this.minValue = minValue;
        this.maxValue = maxValue;

        fieldValueSupplierMap = ImmutableMap.<String, Supplier<String>>builder()
                .put(StatisticConfiguration.FIELD_NAME_DATE_TIME, () -> Long.toString(delegate.getTimeMs()))
                .put(StatisticConfiguration.FIELD_NAME_PRECISION, this::getPrecision)
                .put(StatisticConfiguration.FIELD_NAME_PRECISION_MS, () -> Long.toString(delegate.getPrecisionMs()))
                .put(StatisticConfiguration.FIELD_NAME_COUNT, () -> Long.toString(count))
                .put(StatisticConfiguration.FIELD_NAME_VALUE, () -> Double.toString(value))
                .put(StatisticConfiguration.FIELD_NAME_MIN_VALUE, () -> Double.toString(minValue))
                .put(StatisticConfiguration.FIELD_NAME_MAX_VALUE, () -> Double.toString(maxValue))
                .build();
    }

    @Override
    public long getTimeMs() {
        return delegate.getTimeMs();
    }

    @Override
    public long getPrecisionMs() {
        return delegate.getPrecisionMs();
    }

    @Override
    public List<StatisticTag> getTags() {
        return delegate.getTags();
    }

    @Override
    public Map<String, String> getTagsAsMap() {
        return  delegate.getTagsAsMap();
    }

    public long getCount() {
        return count;
    }

    public double getValue() {
        return value;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    @Override
    public StatisticType getStatisticType() {
        return STATISTIC_TYPE;
    }

    @Override
    public String getFieldValue(final String fieldName) {
        Supplier<String> fieldValueSupplier = fieldValueSupplierMap.get(fieldName);

        if (fieldValueSupplier == null) {
            //either it is a tag field or we don't know what it is

            return delegate.getFieldValue(fieldName);
        } else {
            return Preconditions.checkNotNull(fieldValueSupplier, "Unknown field %s", fieldName).get();
        }
    }

    @Override
    public String toString() {
        return "ValueStatisticDataPoint{" +
                "delegate=" + delegate +
                ", count=" + count +
                ", value=" + value +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ValueStatisticDataPoint that = (ValueStatisticDataPoint) o;

        if (count != that.count) return false;
        if (Double.compare(that.value, value) != 0) return false;
        if (Double.compare(that.minValue, minValue) != 0) return false;
        if (Double.compare(that.maxValue, maxValue) != 0) return false;
        return delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = delegate.hashCode();
        result = 31 * result + (int) (count ^ (count >>> 32));
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
