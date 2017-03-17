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

package stroom.stats.hbase;

import com.google.inject.Injector;
import org.junit.Test;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class HBaseDataLoadIT extends AbstractAppIT {

    @Test
    public void test() {
        Injector injector = getApp().getInjector();
        StatisticsService statisticsService = injector.getInstance(StatisticsService.class);
        UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);

        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        long timeMs = ZonedDateTime.now().toInstant().toEpochMilli();
        List<AggregatedEvent> aggregatedEvents = new ArrayList<>();

        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString();
        UID statName = uniqueIdCache.getOrCreateId(statNameStr);
        assertThat(statName).isNotNull();

        UID tag1 = uniqueIdCache.getOrCreateId("tag1");
        UID tag1val1 = uniqueIdCache.getOrCreateId("tag1val1");
        UID tag2 = uniqueIdCache.getOrCreateId("tag2");
        UID tag2val1 = uniqueIdCache.getOrCreateId("tag2val1");

        StatKey statKey = new StatKey( statName,
                rollUpBitMask,
                interval,
                timeMs,
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val1));

        StatAggregate statAggregate = new CountAggregate(100L);

        AggregatedEvent aggregatedEvent = new AggregatedEvent(statKey, statAggregate);

        aggregatedEvents.add(aggregatedEvent);

        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);
    }





}
