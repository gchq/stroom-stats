package stroom.stats.streams;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.test.StatKeyHelper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.IntStream;

public class TestStatAggregator {

    @Test
    public void add_singleOutputAggregate() throws Exception {

        LocalDateTime baseTime = LocalDateTime.of(2016, 2, 15, 10, 2, 0);
        EventStoreTimeIntervalEnum aggregationInterval = EventStoreTimeIntervalEnum.MINUTE;
                StatAggregator statAggregator = new StatAggregator(10, 10, aggregationInterval);
        long statValue = 10L;
        int loopSize = 10;
        IntStream.rangeClosed(1,loopSize).forEach(i -> {
            LocalDateTime time = baseTime.plusSeconds(i);
            StatKey statKey = StatKeyHelper.buildStatKey(time, aggregationInterval);
            StatAggregate statAggregate = new CountAggregate(statValue);
            statAggregator.add(statKey, statAggregate);
        });

        Assertions.assertThat(statAggregator.size()).isEqualTo(1);

        List<AggregatedEvent> aggregatedEvents = statAggregator.getAll();

        Assertions.assertThat(aggregatedEvents).hasSize(1);

        CountAggregate countAggregate = (CountAggregate) aggregatedEvents.get(0).getStatAggregate();
        Assertions.assertThat(countAggregate.getAggregatedCount())
                .isEqualTo(loopSize * statValue);
        Assertions.assertThat(aggregatedEvents.get(0).getStatKey().getTimeMs())
                .isEqualTo(baseTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    @Test
    public void add_twoOutputAggregates() throws Exception {

        LocalDateTime baseTime1 = LocalDateTime.of(2016, 2, 15, 10, 2, 0);
        LocalDateTime baseTime2 = baseTime1.plusMinutes(1);
        EventStoreTimeIntervalEnum aggregationInterval = EventStoreTimeIntervalEnum.MINUTE;
        StatAggregator statAggregator = new StatAggregator(10, 10, aggregationInterval);
        long statValue1 = 10L;
        long statValue2 = 20L;
        int loopSize = 10;
        IntStream.rangeClosed(1,loopSize).forEach(i -> {
            LocalDateTime time1 = baseTime1.plusSeconds(i);
            StatKey statKey1 = StatKeyHelper.buildStatKey(time1, aggregationInterval);
            StatAggregate statAggregate1 = new CountAggregate(statValue1);
            statAggregator.add(statKey1, statAggregate1);

            LocalDateTime time2 = baseTime2.plusSeconds(i);
            StatKey statKey2 = StatKeyHelper.buildStatKey(time2, aggregationInterval);
            StatAggregate statAggregate2 = new CountAggregate(statValue2);
            statAggregator.add(statKey2, statAggregate2);
        });

        Assertions.assertThat(statAggregator.size()).isEqualTo(2);

        List<AggregatedEvent> aggregatedEvents = statAggregator.getAll();

        Assertions.assertThat(aggregatedEvents).hasSize(2);

        AggregatedEvent expectedEvent1 = new AggregatedEvent(
                StatKeyHelper.buildStatKey(baseTime1, aggregationInterval),
                new CountAggregate(statValue1 * loopSize));

        AggregatedEvent expectedEvent2 = new AggregatedEvent(
                StatKeyHelper.buildStatKey(baseTime2, aggregationInterval),
                new CountAggregate(statValue2 * loopSize));

        Assertions.assertThat(aggregatedEvents).containsExactlyInAnyOrder(expectedEvent1, expectedEvent2);


        Assertions.assertThat(statAggregator.stream().count()).isEqualTo(2);

        statAggregator.clear();

        Assertions.assertThat(statAggregator.size()).isEqualTo(0);
        Assertions.assertThat(statAggregator.getAll()).hasSize(0);
        Assertions.assertThat(statAggregator.stream().count()).isEqualTo(0);
    }

}