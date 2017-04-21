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
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicStatisticDataPoint implements StatisticDataPoint {

    private final String statisticName;
    private final long timeMs;
    private final long precisionMs;
    private final List<StatisticTag> tags;
    private final Map<String, String> tagToValueMap;


    public BasicStatisticDataPoint(final String statisticName, final long timeMs, final long precisionMs, final List<StatisticTag> tags) {
        Preconditions.checkNotNull(statisticName);
        Preconditions.checkArgument(timeMs >= 0);
        Preconditions.checkArgument(precisionMs >= 0);
        Preconditions.checkNotNull(tags);

        this.statisticName = statisticName;
        this.timeMs = timeMs;
        this.precisionMs = precisionMs;
        this.tags = tags;

        if (tags.isEmpty()) {
            this.tagToValueMap = Collections.emptyMap();
        } else {
            this.tagToValueMap = new HashMap<>();
            for (StatisticTag tag : tags) {
                this.tagToValueMap.put(tag.getTag(), tag.getValue());
            }
        }
    }

    public String getStatisticName() {
        return statisticName;
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

    @Override
    public StatisticType getStatisticType() {
        throw new UnsupportedOperationException("A BasicStatisticDataPoint has no type");
    }

//    @Override
//    public Map<String, Object> getFieldToValueMap() {
//        return tagToValueMap;
//    }

    @Override
    public String getFieldValue(final String fieldName) {
        return tagToValueMap.get(fieldName);
    }

    @Override
    public String toString() {
        return "BasicStatisticDataPoint{" +
                "statisticName=" + statisticName +
                ", timeMs=" + timeMs +
                ", precisionMs=" + precisionMs +
                ", tags=" + tags +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BasicStatisticDataPoint that = (BasicStatisticDataPoint) o;

        if (timeMs != that.timeMs) return false;
        if (precisionMs != that.precisionMs) return false;
        if (!statisticName.equals(that.statisticName)) return false;
        if (!tags.equals(that.tags)) return false;
        return tagToValueMap.equals(that.tagToValueMap);
    }

    @Override
    public int hashCode() {
        int result = statisticName.hashCode();
        result = 31 * result + (int) (timeMs ^ (timeMs >>> 32));
        result = 31 * result + (int) (precisionMs ^ (precisionMs >>> 32));
        result = 31 * result + tags.hashCode();
        result = 31 * result + tagToValueMap.hashCode();
        return result;
    }
}
