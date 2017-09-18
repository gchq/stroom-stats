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

package stroom.stats.streams;

import com.google.common.base.Preconditions;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@NotThreadSafe
class StatAggregator {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatAggregator.class);

    private Map<StatEventKey, StatAggregate> buffer;
    private final int minSize;
    private final Instant expiryTime;
    private final EventStoreTimeIntervalEnum aggregationInterval;
    private int inputCount = 0;


    /**
     * @param minSize The minimum number of reduced aggregates in the aggregator before it is deemed ready to be flushed
     * @param aggregationInterval
     * @param timeToLiveMs
     */
    public StatAggregator(final int minSize,
                          final EventStoreTimeIntervalEnum aggregationInterval,
                          final long timeToLiveMs) {
        //initial size to avoid it rehashing. x1.2 to allow for it going a bit over the min value
        this.buffer = new HashMap<>((int) Math.ceil((minSize * 1.2) / 0.75));
        this.minSize = minSize;
        this.expiryTime = Instant.now().plusMillis(timeToLiveMs);
        this.aggregationInterval = aggregationInterval;
    }

    /**
     * Add a single key/aggregate pair into the aggregator. The aggregate will be aggregated
     * with any existing aggregates for that {@link StatEventKey}
     */
    public void add(final StatEventKey statEventKey, final StatAggregate statAggregate) {

        Preconditions.checkNotNull(statEventKey);
        Preconditions.checkNotNull(statAggregate);
        Preconditions.checkArgument(statEventKey.getInterval().equals(aggregationInterval),
                "statEventKey %s doesn't match aggregator interval %s", statEventKey, aggregationInterval);

        LOGGER.trace("Adding statEventKey {} and statAggregate {} to aggregator {}", statEventKey, statAggregate, aggregationInterval);

        //The passed StatEventKey will already have its time truncated to the interval of this aggregator
        //so we don't need to do anything to it.

        inputCount++;

        //aggregate the passed aggregate and key into the existing aggregates
        buffer.merge(
                statEventKey,
                statAggregate,
                StatAggregate::aggregatePair);
//                (existingAgg, newAgg) -> existingAgg.aggregate(newAgg));
    }

    public int size() {
        return buffer.size();
    }

    public int getInputCount() {
        return inputCount;
    }

    public Instant getExpiryTime() {
        return expiryTime;
    }

    /**
     * @return The compression savings expressed as a percentage. 0% = no compression, 100% = total compression
     */
    public double getAggregationPercentage() {
        if (buffer.isEmpty()) {
            return 0;
        } else {
            return 100 - ((double) buffer.size() / inputCount * 100);
        }
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    /**
     * @return True if the number of reduced records in the aggregator has reached minSize or timeToLiveMs has
     * been passed.
     */
    public boolean isReadyForFlush() {
        return (Instant.now().isAfter(expiryTime) || buffer.size() > minSize);
    }

    public EventStoreTimeIntervalEnum getAggregationInterval() {
        return aggregationInterval;
    }

    public Map<StatEventKey, StatAggregate> getAggregates() {
        LOGGER.trace(() -> String.format("getAggregates called, return %s events", buffer.size()));

        return buffer;
    }

    @Override
    public String toString() {
        return "StatAggregator{" +
                "minSize=" + minSize +
                ", expiredTime=" + expiryTime +
                ", aggregationInterval=" + aggregationInterval +
                ", current size=" + buffer.size() +
                ", inputCount=" + inputCount +
                '}';
    }
}
