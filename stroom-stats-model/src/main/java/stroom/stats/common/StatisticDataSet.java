

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

import stroom.stats.api.StatisticType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class StatisticDataSet implements Iterable<StatisticDataPoint> {
    private final String statisticName;
    private final StatisticType statisticType;
    private final Set<StatisticDataPoint> statisticDataPoints;

    public StatisticDataSet(final String statisticName, final StatisticType statisticType) {
        this.statisticName = statisticName;
        this.statisticType = statisticType;
        this.statisticDataPoints = new HashSet<>();
    }

    public StatisticDataSet(final String statisticName, final StatisticType statisticType,
                            final Set<StatisticDataPoint> statisticDataPoints) {
        for (StatisticDataPoint dataPoint : statisticDataPoints) {
            if (!statisticType.equals(dataPoint.getStatisticType())) {
                throw new RuntimeException(
                        "Attempting to create a StatisticDataSet with StatisticDataPoints of an incompatible StatisticType");
            }
        }

        this.statisticName = statisticName;
        this.statisticType = statisticType;
        this.statisticDataPoints = statisticDataPoints;
    }

    public void addDataPoint(StatisticDataPoint dataPoint) {
        if (!statisticType.equals(dataPoint.getStatisticType())) {
            throw new RuntimeException("Attempting to add a StatisticDataPoint of an incompatible StatisticType");
        }

        this.statisticDataPoints.add(dataPoint);
    }

    public String getStatisticName() {
        return statisticName;
    }

    public StatisticType getStatisticType() {
        return statisticType;
    }

    public Set<StatisticDataPoint> getStatisticDataPoints() {
        return statisticDataPoints;
    }

    public int size() {
        return statisticDataPoints.size();
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
        result = prime * result + ((statisticName == null) ? 0 : statisticName.hashCode());
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
        if (statisticName == null) {
            if (other.statisticName != null)
                return false;
        } else if (!statisticName.equals(other.statisticName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "StatisticDataSet [statisticName=" + statisticName + ", statisticType=" + statisticType
                + ", statisticDataPoints size=" + statisticDataPoints.size() + "]";
    }

}
