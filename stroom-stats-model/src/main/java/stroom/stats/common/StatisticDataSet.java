

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
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;


public class StatisticDataSet implements Iterable<StatisticDataPoint> {
    private final StatisticConfiguration statisticConfiguration;
    private final EventStoreTimeIntervalEnum timeInterval;

    private final List<StatisticDataPoint> statisticDataPoints;

    public StatisticDataSet(final StatisticConfiguration statisticConfiguration,
                            final EventStoreTimeIntervalEnum timeInterval) {

        Preconditions.checkNotNull(statisticConfiguration);
        Preconditions.checkNotNull(timeInterval);

        this.statisticConfiguration = statisticConfiguration;
        this.timeInterval = timeInterval;
        this.statisticDataPoints = new ArrayList<>();
    }

//    public StatisticDataSet(final String statisticUuid,
//                            final StatisticType statisticType,
//                            final List<StatisticDataPoint> statisticDataPoints) {
//        if (Preconditions.checkNotNull(statisticDataPoints).stream()
//                .anyMatch(point -> !statisticType.equals(point.getStatisticType()))) {
//            throw new RuntimeException(
//                    "Attempting to create a StatisticDataSet with StatisticDataPoints of an incompatible StatisticType");
//        }
//
//        this.statisticUuid = statisticUuid;
//        this.statisticType = statisticType;
//        this.statisticDataPoints = statisticDataPoints;
//    }

    public StatisticDataSet addDataPoint(StatisticDataPoint dataPoint) {
        Preconditions.checkNotNull(dataPoint);
        //datapoint must be for the same statConfig as the dataset as a whole
        Preconditions.checkArgument(dataPoint.getStatisticConfiguration().getUuid().equals(statisticConfiguration.getUuid()));
        Preconditions.checkArgument(dataPoint.getTimeInterval().equals(timeInterval));

        this.statisticDataPoints.add(dataPoint);
        return this;
    }

    public StatisticConfiguration getStatisticConfiguration() {
        return statisticConfiguration;
    }

    public EventStoreTimeIntervalEnum getTimeInterval() {
        return timeInterval;
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
    public String toString() {
        return "StatisticDataSet{" +
                "statisticConfiguration=" + statisticConfiguration +
                ", timeInterval=" + timeInterval +
                ", statisticDataPoints=" + statisticDataPoints +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final StatisticDataSet that = (StatisticDataSet) o;

        if (!statisticConfiguration.equals(that.statisticConfiguration)) return false;
        if (timeInterval != that.timeInterval) return false;
        return statisticDataPoints.equals(that.statisticDataPoints);
    }

    @Override
    public int hashCode() {
        int result = statisticConfiguration.hashCode();
        result = 31 * result + timeInterval.hashCode();
        result = 31 * result + statisticDataPoints.hashCode();
        return result;
    }
}
