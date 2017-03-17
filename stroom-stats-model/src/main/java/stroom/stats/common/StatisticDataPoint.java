

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

import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Value object to hold a statistic data point retreived from a statistic store.
 * This differs from a {@link StatisticEvent} in that this data point may
 * represent an aggregated value rather than a single statistic event.
 */
public class StatisticDataPoint {
    private final long timeMs;
    private final long precisionMs;
    private final List<StatisticTag> tags;
    private final long count;
    private final double value;
    private final double minValue;
    private final double maxValue;
    private final StatisticType statisticType;

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
     * @return A populated {@link StatisticDataPoint} instance
     */
    public static StatisticDataPoint valueInstance(final long timeMs, final long precisionMs,
            final List<StatisticTag> tags, final double value, final long count, final double minValue,
            final double maxValue) {
        return new StatisticDataPoint(timeMs, precisionMs, tags, count, value, minValue, maxValue, StatisticType.VALUE);
    }

    /**
     * Constructor for a count type statistic data point
     *
     * @param timeMs
     *            The timestamp of the aggregated data point
     * @param tags
     *            The list of tav/value pairs that qualify the data point
     * @param count
     *            The count of the number of statistic events that have happened
     *            in this period
     * @return A populated {@link StatisticDataPoint} instance
     */
    public static StatisticDataPoint countInstance(final long timeMs, final long precisionMs,
            final List<StatisticTag> tags, final long count) {
        return new StatisticDataPoint(timeMs, precisionMs, tags, count, 0D, 0, 0, StatisticType.COUNT);
    }

    // private StatisticDataPoint() {
    //
    // this.timeMs = 0;
    // this.count = 0L;
    // this.value = 0D;
    // this.minValue = 0;
    // this.maxValue = 0;
    // }

    private StatisticDataPoint(final long timeMs, final long precisionMs, final List<StatisticTag> tags,
            final Long count, final Double value, final double minValue, final double maxValue,
            StatisticType statisticType) {
        this.timeMs = timeMs;
        this.precisionMs = precisionMs;
        this.tags = tags;
        this.count = count;
        this.value = value;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.statisticType = statisticType;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public long getPrecisionMs() {
        return precisionMs;
    }

    public List<StatisticTag> getTags() {
        return tags;
    }

    public Map<String, String> getTagsAsMap() {
        Map<String, String> map = new HashMap<>();
        for (StatisticTag tag : tags) {
            map.put(tag.getTag(), tag.getValue());
        }
        return map;
    }

    public Long getCount() {
        return count;
    }

    public Double getValue() {
        if (!statisticType.equals(StatisticType.VALUE))
            throw new UnsupportedOperationException("Method only support for value type statistics");

        return value;
    }

    public double getMinValue() {
        if (!statisticType.equals(StatisticType.VALUE))
            throw new UnsupportedOperationException("Method only support for value type statistics");

        return minValue;
    }

    public double getMaxValue() {
        if (!statisticType.equals(StatisticType.VALUE))
            throw new UnsupportedOperationException("Method only support for value type statistics");

        return maxValue;
    }

    public StatisticType getStatisticType() {
        return statisticType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (count ^ (count >>> 32));
        long temp;
        temp = Double.doubleToLongBits(maxValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + (int) (precisionMs ^ (precisionMs >>> 32));
        result = prime * result + ((statisticType == null) ? 0 : statisticType.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        result = prime * result + (int) (timeMs ^ (timeMs >>> 32));
        temp = Double.doubleToLongBits(value);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StatisticDataPoint other = (StatisticDataPoint) obj;
        if (count != other.count)
            return false;
        if (Double.doubleToLongBits(maxValue) != Double.doubleToLongBits(other.maxValue))
            return false;
        if (Double.doubleToLongBits(minValue) != Double.doubleToLongBits(other.minValue))
            return false;
        if (precisionMs != other.precisionMs)
            return false;
        if (statisticType != other.statisticType)
            return false;
        if (tags == null) {
            if (other.tags != null)
                return false;
        } else if (!tags.equals(other.tags))
            return false;
        if (timeMs != other.timeMs)
            return false;
        if (Double.doubleToLongBits(value) != Double.doubleToLongBits(other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "StatisticDataPoint [timeMs=" + timeMs + ", precisionMs=" + precisionMs + ", tags=" + tags + ", count="
                + count + ", value=" + value + ", minValue=" + minValue + ", maxValue=" + maxValue + ", statisticType="
                + statisticType + "]";
    }

}
