/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.streams.aggregation;

import stroom.stats.api.MultiPartIdentifier;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

@NotThreadSafe
public class CountAggregate extends StatAggregate {
    long aggregatedCount = 0L;

    @SuppressWarnings("unused") //needed for aggregate initialisation in the streams
    public CountAggregate() {
        super();
    }

    public CountAggregate(final List<MultiPartIdentifier> eventIds, final long aggregatedCount) {
        super(eventIds);
        this.aggregatedCount = aggregatedCount;
    }

    @Override
    public StatAggregate aggregate(final StatAggregate other, final int maxEventIds) {
        aggregateEventIds(other, maxEventIds);
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
