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

import java.util.ArrayList;
import java.util.List;

public abstract class StatAggregate {

    public static final String PROP_KEY_MAX_AGGREGATED_EVENT_IDS = "stroom.stats.aggregation.maxEventIds";

    protected final List<MultiPartIdentifier> eventIds;

    public abstract StatAggregate aggregate(final StatAggregate other, final int maxEventIds);

    public StatAggregate() {
        eventIds = new ArrayList<>();
    }

    public StatAggregate(final List<MultiPartIdentifier> eventIds) {
        this.eventIds = eventIds;
    }

    void aggregateEventIds(final StatAggregate other, final int maxEventIds) {
        //limit the number of event Ids we hold to prevent noisy events creating massive aggregates
        int addCount = maxEventIds - eventIds.size();
        eventIds.addAll(other.eventIds.subList(0, addCount));
    }

    public List<MultiPartIdentifier> getEventIds() {
        return eventIds;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatAggregate that = (StatAggregate) o;

        return eventIds.equals(that.eventIds);
    }

    @Override
    public int hashCode() {
        return eventIds.hashCode();
    }

    @Override
    public String toString() {
        return "StatAggregate{" +
                "eventIds=" + eventIds +
                '}';
    }
}
