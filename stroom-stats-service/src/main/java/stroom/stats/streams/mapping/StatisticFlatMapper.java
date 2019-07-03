package stroom.stats.streams.mapping;

import org.apache.kafka.streams.KeyValue;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.aggregation.StatAggregate;

public interface StatisticFlatMapper {
    String NULL_VALUE_STRING = "<<<<NULL_VALUE>>>>";

    static boolean isInsidePurgeRetention(StatisticWrapper statisticWrapper,
                                          EventStoreTimeIntervalEnum interval,
                                          int retentionRowIntervals) {

        // round start time down to the last row key interval
        final long rowInterval = interval.rowKeyInterval();
        final long roundedNow = ((long) (System.currentTimeMillis() / rowInterval)) * rowInterval;
        final long cutOffTime = roundedNow - (rowInterval * retentionRowIntervals);

        return statisticWrapper.getTimeMs() >= cutOffTime;
    }

    Iterable<KeyValue<StatEventKey, StatAggregate>> flatMap(String statUuid, StatisticWrapper statisticWrapper);
}
