

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

package stroom.stats.hbase.aggregator;

import stroom.stats.api.StatisticType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class MockEventStoresThreadFlushAggregator implements EventStoresThreadFlushAggregator {
    private final List<StatisticType> statTypeList = new ArrayList<>();
    private final List<AbstractInMemoryEventStore> storeList = new ArrayList<>();
    private final AtomicInteger callCount = new AtomicInteger(0);

    @Override
    public void addFlushedStatistics(final AbstractInMemoryEventStore storeToFlush) {
        statTypeList.add(storeToFlush.getEventStoreMapKey().getStatisticType());
        storeList.add(storeToFlush);
        callCount.incrementAndGet();
    }

    @Override
    public void flushAll() {
        // do nothing as for this mock we don't care

    }

    @Override
    public void flushQueue() {

    }

    public List<StatisticType> getStatTypeList() {
        return statTypeList;
    }

    public List<AbstractInMemoryEventStore> getStoreList() {
        return storeList;
    }

    public int getCallCount() {
        return callCount.get();
    }

    public void resetCallData() {
        statTypeList.clear();
        storeList.clear();
        callCount.set(0);
    }


    @Override
    public void enableDisableIdPool(final StatisticType statisticType) {
    }

    @Override
    public Map<EventStoreMapKey, Integer> getEventStoreSizes() {
        return new HashMap<>();
    }
}
