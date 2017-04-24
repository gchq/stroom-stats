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
import java.util.List;

@NotThreadSafe
public class CountAggregate extends StatAggregate {
    long aggregatedCount = 0L;

    public CountAggregate(final long aggregatedCount) {
        super();
        this.aggregatedCount = aggregatedCount;
    }

    public CountAggregate(final List<MultiPartIdentifier> eventIds, final int maxEventIds, final long aggregatedCount) {
        super(eventIds, maxEventIds);
        this.aggregatedCount = aggregatedCount;
    }

    @Override
    public StatAggregate aggregate(final StatAggregate other) {
        aggregateEventIds(other);
        try {
            this.aggregatedCount += ((CountAggregate)other).aggregatedCount;
        } catch (ClassCastException e) {
            throw new RuntimeException(String.format("Supplied StatAggregate %s is not a CountAggregate", other.getClass().getName()), e);
        }
        return this;
    }

    public long getAggregatedCount() {
        return aggregatedCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CountAggregate that = (CountAggregate) o;

        return aggregatedCount == that.aggregatedCount;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (aggregatedCount ^ (aggregatedCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CountAggregate{" +
                "aggregatedCount=" + aggregatedCount +
                '}';
    }
}
