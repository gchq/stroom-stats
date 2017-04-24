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

import com.google.common.base.Preconditions;
import stroom.stats.api.MultiPartIdentifier;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;

@NotThreadSafe
public abstract class StatAggregate {

    //TODO probably should refactor this to be an interface with a new implementing class of EventIdAggregate
    //which both CountAggregate and ValueAggregate can include via composition

    public static final String PROP_KEY_MAX_AGGREGATED_EVENT_IDS = "stroom.stats.aggregation.maxEventIds";

    private final int maxEventIds;
    protected final List<MultiPartIdentifier> eventIds;

    /**
     * @param other
     * @return Aggregates the content of aggregate2 into this, returning the mutated this
     */
    public abstract <T extends StatAggregate> T aggregate(final T other);

    /**
     * @return Aggregates the content of aggregate2 into aggregate1, returning the mutated aggregate1
     */
    public static <T extends StatAggregate> T aggregatePair(final T aggregate1, final T aggregate2) {

        //Named aggregatePair rather than aggregate to allow it to be used as a method reference
        return aggregate1.aggregate(aggregate2);
    }

    public StatAggregate() {
        eventIds = new ArrayList<>();
        maxEventIds = Integer.MAX_VALUE;
    }

    /**
     * @param eventIds A list of eventIds that contributed to the {@link StatAggregate}
     * @param maxEventIds The maximum number of eventIds to hold in the {@link StatAggregate}, if eventIds is above
     *                    this limit, then only the first N will be included in the aggregate
     */
    public StatAggregate(final List<MultiPartIdentifier> eventIds, final int maxEventIds) {
        Preconditions.checkNotNull(eventIds);
        Preconditions.checkArgument(maxEventIds >= 0);
        this.maxEventIds = maxEventIds;
        if (eventIds.size() > maxEventIds) {
            this.eventIds = new ArrayList<>(eventIds.subList(0, maxEventIds));
        } else {
            this.eventIds = new ArrayList<>(eventIds);
        }
    }

    /**
     * Aggregate the eventIds of other into this, mutating this
     * @param other The other {@link StatAggregate} whose eventIds will be aggregated into this
     */
    void aggregateEventIds(final StatAggregate other) {
        //limit the number of event Ids we hold to prevent noisy events creating massive aggregates
        //Use the maxEventIds of this rather than other
        try {
            if (other.eventIds.size() > 0) {
                int addCount = maxEventIds - eventIds.size();
                eventIds.addAll(other.eventIds.subList(0, Math.min(other.eventIds.size(), addCount)));
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error aggregating event IDs, this: %s, other: %s", this, other), e);
        }
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
