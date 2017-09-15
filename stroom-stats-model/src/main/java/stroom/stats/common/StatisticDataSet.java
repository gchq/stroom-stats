

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
import stroom.stats.api.StatisticType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;


public class StatisticDataSet implements Iterable<StatisticDataPoint> {
    private final String statisticUuid;
    private final StatisticType statisticType;
    private final List<StatisticDataPoint> statisticDataPoints;

    public StatisticDataSet(final String statisticUuid, final StatisticType statisticType) {
        this.statisticUuid = statisticUuid;
        this.statisticType = statisticType;
        this.statisticDataPoints = new ArrayList<>();
    }

    public StatisticDataSet(final String statisticUuid,
                            final StatisticType statisticType,
                            final List<StatisticDataPoint> statisticDataPoints) {
        if (Preconditions.checkNotNull(statisticDataPoints).stream()
                .anyMatch(point -> !statisticType.equals(point.getStatisticType()))) {
            throw new RuntimeException(
                    "Attempting to create a StatisticDataSet with StatisticDataPoints of an incompatible StatisticType");
        }

        this.statisticUuid = statisticUuid;
        this.statisticType = statisticType;
        this.statisticDataPoints = statisticDataPoints;
    }

    public StatisticDataSet addDataPoint(StatisticDataPoint dataPoint) {
        Preconditions.checkArgument(statisticType.equals(dataPoint.getStatisticType()),
                "Attempting to add a StatisticDataPoint of an incompatible StatisticType");

        this.statisticDataPoints.add(dataPoint);
        return this;
    }

    public String getStatisticUuid() {
        return statisticUuid;
    }

    public StatisticType getStatisticType() {
        return statisticType;
    }

    public List<StatisticDataPoint> getStatisticDataPoints() {
        return statisticDataPoints;
    }

    public Stream<StatisticDataPoint> stream() {
        return statisticDataPoints.stream();
    }

    public Stream<StatisticDataPoint> parallelStream() {
        return statisticDataPoints.parallelStream();
    }

    public int size() {
        return statisticDataPoints.size();
    }

    public boolean isEmpty() {
        return statisticDataPoints.isEmpty();
    }

    @Override
    public Iterator<StatisticDataPoint> iterator() {
        return statisticDataPoints.iterator();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + ((statisticDataPoints == null) ? 0 : statisticDataPoints.hashCode());
        result = prime * result + ((statisticUuid == null) ? 0 : statisticUuid.hashCode());
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
        StatisticDataSet other = (StatisticDataSet) obj;
        if (statisticDataPoints == null) {
            if (other.statisticDataPoints != null)
                return false;
        } else if (!statisticDataPoints.equals(other.statisticDataPoints))
            return false;
        if (statisticUuid == null) {
            if (other.statisticUuid != null)
                return false;
        } else if (!statisticUuid.equals(other.statisticUuid))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "StatisticDataSet [statisticUuid=" + statisticUuid + ", statisticType=" + statisticType
                + ", statisticDataPoints size=" + statisticDataPoints.size() + "]";
    }

}
