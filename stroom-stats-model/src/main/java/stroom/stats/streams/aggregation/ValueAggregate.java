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

package stroom.stats.streams.aggregation;

import stroom.stats.api.MultiPartIdentifier;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.List;

/**
 * Holds a value aggregate, i.e. a value that cannot be summed, e.g. a rate or a percentage
 * The aggregate is held as a sum of all values along with a aggregatedCount of the number of values
 * that contributed to that sum. The sum and aggregatedCount can be used to compute an average value.
 * A min and max value are also held.
 */
@NotThreadSafe
public class ValueAggregate extends StatAggregate {

    private int count;
    private double aggregatedValue;
    private double minValue;
    private double maxValue;

    @SuppressWarnings("unused") //needed for aggregate initialisation in kafka streams
    public ValueAggregate() {
    }

    public ValueAggregate(final double aggregatedValue) {
        this(Collections.emptyList(), aggregatedValue);
    }
    /**
     * Basic constructor for a value that has NOT already been aggregated
     */
    public ValueAggregate(final List<MultiPartIdentifier> eventIds, final double aggregatedValue) {
        super(eventIds);
        this.aggregatedValue = aggregatedValue;
        this.minValue = aggregatedValue;
        this.maxValue = aggregatedValue;
        this.count = 1;
    }

    @Override
    public StatAggregate aggregate(final StatAggregate other, final int maxEventIds) {
        aggregateEventIds(other, maxEventIds);
        try {
            ValueAggregate otherValueAggregate = (ValueAggregate) other;
            this.count += otherValueAggregate.count;
            this.aggregatedValue += otherValueAggregate.aggregatedValue;
            this.minValue = min(this.minValue, otherValueAggregate.minValue);
            this.maxValue = max(this.maxValue, otherValueAggregate.maxValue);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Supplied StatAggregate %s is not a ValueAggregate", other.getClass().getName()), e);
        }
        return this;
    }

    private double min(final double val1, final double val2) {
        return val2 > val1 ? val1 : val2;
    }

    private double max(final double val1, final double val2) {
        return val2 < val1 ? val1 : val2;
    }

    public int getCount() {
        return count;
    }

    public double getAggregatedValue() {
        return aggregatedValue;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ValueAggregate that = (ValueAggregate) o;

        if (count != that.count) return false;
        if (Double.compare(that.aggregatedValue, aggregatedValue) != 0) return false;
        if (Double.compare(that.minValue, minValue) != 0) return false;
        return Double.compare(that.maxValue, maxValue) == 0;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        result = 31 * result + count;
        temp = Double.doubleToLongBits(aggregatedValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ValueAggregate{" +
                "aggregatedCount=" + count +
                ", aggregatedValue=" + aggregatedValue +
                ", minValue=" + minValue +
                ", maxValue=" + maxValue +
                '}';
    }
}
