package stroom.stats.streams;

import com.google.common.base.Preconditions;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class StatAggregator {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatAggregator.class);

    private final Map<StatKey, StatAggregate> buffer;
    private final int maxEventIds;
    private final EventStoreTimeIntervalEnum aggregationInterval;

    public StatAggregator(final int expectedSize, final int maxEventIds, final EventStoreTimeIntervalEnum aggregationInterval) {
        //initial size to avoid it rehashing
        this.buffer = new HashMap<>((int)Math.ceil(expectedSize / 0.75));
        this.maxEventIds = maxEventIds;
        this.aggregationInterval = aggregationInterval;
    }

    public void add(final StatKey statKey, final StatAggregate statAggregate){
        Preconditions.checkNotNull(statKey);
        Preconditions.checkNotNull(statAggregate);

        LOGGER.trace("Adding statKey {} and statAggregate {} to aggregator {}", statKey, statAggregate, aggregationInterval);

        statKey.cloneAndTruncateTimeToInterval();

        //aggregate the passed aggregate and key into the existing aggregates
        buffer.merge(
                statKey,
                statAggregate,
                (existingAgg, newAgg) -> existingAgg.aggregate(newAgg, maxEventIds));
    }

    public int size() {
        return buffer.size();
    }

    public void clear() {
        LOGGER.trace("Clear called for interval {}", aggregationInterval);
        buffer.clear();
    }

    public EventStoreTimeIntervalEnum getAggregationInterval() {
        return aggregationInterval;
    }

    public Stream<AggregatedEvent> stream() {
        return buffer.entrySet().stream()
                .map(entry -> new AggregatedEvent(entry.getKey(), entry.getValue()));
    }

    public List<AggregatedEvent> getAll() {
        LOGGER.trace(() -> String.format("getAll called, return %s events", buffer.size()));
        return stream().collect(Collectors.toList());
    }
}
