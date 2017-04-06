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

import javaslang.Tuple2;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.test.StatKeyHelper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestStatAggregator {

    @Test
    public void add_singleOutputAggregate() throws Exception {

        LocalDateTime baseTime = LocalDateTime.of(2016, 2, 15, 10, 2, 0);
        EventStoreTimeIntervalEnum aggregationInterval = EventStoreTimeIntervalEnum.MINUTE;
                StatAggregator statAggregator = new StatAggregator(10, 10, aggregationInterval, 10_000);
        long statValue = 10L;
        int loopSize = 10;
        IntStream.rangeClosed(1,loopSize).forEach(i -> {
            LocalDateTime time = baseTime.plusSeconds(i);
            StatKey statKey = StatKeyHelper.buildStatKey(time, aggregationInterval);
            StatAggregate statAggregate = new CountAggregate(statValue);
            statAggregator.add(statKey, statAggregate);
        });

        Assertions.assertThat(statAggregator.size()).isEqualTo(1);

        Map<StatKey, StatAggregate> aggregatedEvents = statAggregator.getAggregates();

        Assertions.assertThat(aggregatedEvents).hasSize(1);

        CountAggregate countAggregate = (CountAggregate) aggregatedEvents.values().stream().findFirst().get();
        Assertions.assertThat(countAggregate.getAggregatedCount())
                .isEqualTo(loopSize * statValue);
        Assertions.assertThat(aggregatedEvents.keySet().stream().findFirst().get().getTimeMs())
                .isEqualTo(baseTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    @Test
    public void add_twoOutputAggregates() throws Exception {

        LocalDateTime baseTime1 = LocalDateTime.of(2016, 2, 15, 10, 2, 0);
        LocalDateTime baseTime2 = baseTime1.plusMinutes(1);
        EventStoreTimeIntervalEnum aggregationInterval = EventStoreTimeIntervalEnum.MINUTE;
        StatAggregator statAggregator = new StatAggregator(10, 10, aggregationInterval, 10_000);
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

        Map<StatKey, StatAggregate> aggregatedEvents = statAggregator.getAggregates();
        List<Tuple2<StatKey, StatAggregate>> pairs = aggregatedEvents.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        Assertions.assertThat(aggregatedEvents).hasSize(2);

        Tuple2<StatKey, StatAggregate> expectedEvent1 = new Tuple2<>(
                StatKeyHelper.buildStatKey(baseTime1, aggregationInterval),
                new CountAggregate(statValue1 * loopSize));

        Tuple2<StatKey, StatAggregate> expectedEvent2 = new Tuple2<>(
                StatKeyHelper.buildStatKey(baseTime2, aggregationInterval),
                new CountAggregate(statValue2 * loopSize));

        Assertions.assertThat(pairs).containsExactlyInAnyOrder(expectedEvent1, expectedEvent2);

    }

}