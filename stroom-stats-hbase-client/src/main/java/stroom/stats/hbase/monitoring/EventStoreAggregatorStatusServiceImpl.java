

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

package stroom.stats.hbase.monitoring;

import stroom.stats.api.StatisticType;
import stroom.stats.hbase.EventStores;
import stroom.stats.hbase.aggregator.EventStoreMapKey;
import stroom.stats.hbase.aggregator.EventStoresPutAggregator;
import stroom.stats.hbase.aggregator.EventStoresThreadFlushAggregator;
import stroom.stats.hbase.aggregator.InMemoryEventStoreIdPool;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.shared.hbase.monitoring.EventStoreAggregatorStatus;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class EventStoreAggregatorStatusServiceImpl implements EventStoreAggregatorStatusService {
    private final EventStoresPutAggregator putAggregator;
    private final EventStoresThreadFlushAggregator threadAggregator;
    private final EventStores eventStores;
    private final InMemoryEventStoreIdPool eventStoreIdPool;
    private final StroomPropertyService propertyService;

    @Inject
    public EventStoreAggregatorStatusServiceImpl(final EventStoresPutAggregator putAggregator,
            final EventStoresThreadFlushAggregator threadAggregator, final EventStores eventStores,
            final InMemoryEventStoreIdPool eventStoreIdPool, final StroomPropertyService propertyService) {
        this.putAggregator = putAggregator;
        this.threadAggregator = threadAggregator;
        this.eventStores = eventStores;
        this.eventStoreIdPool = eventStoreIdPool;
        this.propertyService = propertyService;
    }

    @Override
    public List<EventStoreAggregatorStatus> getStatusList() {
        final List<EventStoreAggregatorStatus> list = new ArrayList<>();

            addIdPoolStats(list, 1);
            addPutAggregatorStatuses(list, 2);
            addThreadAggregatorStatuses(list, 3);
//            addPutBufferRowCounts(list, 4);
//            addPutBufferCellCounts(list, 5);
//            addBatchPutTaskPermitCount(list, 6);
            addPutCounts(list, 7);
        return list;
    }

    private void addIdPoolStats(final List<EventStoreAggregatorStatus> list, final int orderNo) {
        for (final StatisticType statisticType : StatisticType.values()) {
            list.add(new EventStoreAggregatorStatus("????", orderNo, "Thread level aggregator ID permits available",
                    "-", null, statisticType.name(),
                    Integer.toString(eventStoreIdPool.getAvailablePermitCount(statisticType))));

            list.add(new EventStoreAggregatorStatus("????", orderNo, "Thread level aggregator ID pool state", "-", null,
                    statisticType.name(), eventStoreIdPool.getEnabledState(statisticType) ? "ENABLED" : "DISABLED"));

        }
    }

    private void addThreadAggregatorStatuses(final List<EventStoreAggregatorStatus> list, final int orderNo) {
        // method always returns an initialised set
        for (final Entry<EventStoreMapKey, Integer> entry : threadAggregator.getEventStoreSizes().entrySet()) {
            final EventStoreMapKey key = entry.getKey();
            list.add(new EventStoreAggregatorStatus("????", orderNo, "Node level aggregator cell count",
                    key.getTimeInterval().longName(), null, key.getStatisticType().name(),
                    Integer.toString(entry.getValue())));
        }
    }

    private void addPutAggregatorStatuses(final List<EventStoreAggregatorStatus> list, final int orderNo) {
        // method always returns an initialised set
        for (final Entry<EventStoreMapKey, Integer> entry : putAggregator.getEventStoreSizes().entrySet()) {
            final EventStoreMapKey key = entry.getKey();
            list.add(new EventStoreAggregatorStatus("????", orderNo, "Thread level aggregator cell count",
                    key.getTimeInterval().longName(), key.getId(), key.getStatisticType().name(),
                    Integer.toString(entry.getValue())));
        }
    }

//    private void addPutBufferRowCounts(final List<EventStoreAggregatorStatus> list, final int orderNo) {
//        // method always returns an initialised set
//        // get counts of the rows in the buffers
//        for (final Entry<EventStoreTimeIntervalEnum, Integer> entry : eventStores.getEventStorePutBufferSizes(false)
//                .entrySet()) {
//            list.add(new EventStoreAggregatorStatus("????", orderNo, "Put buffer row count", entry.getKey().longName(),
//                    null, "COUNT", Integer.toString(entry.getValue())));
//        }
//    }

//    private void addPutBufferCellCounts(final List<EventStoreAggregatorStatus> list, final int orderNo) {
//        // get deep counts of the cells in the buffers
//        for (final Entry<EventStoreTimeIntervalEnum, Integer> entry : eventStores.getEventStorePutBufferSizes(true)
//                .entrySet()) {
//            list.add(new EventStoreAggregatorStatus("????", orderNo, "Put buffer cell count", entry.getKey().longName(),
//                    null, "COUNT", Integer.toString(entry.getValue())));
//        }
//    }

    private void addPutCounts(final List<EventStoreAggregatorStatus> list, final int orderNo) {
        for (final StatisticType statisticType : StatisticType.values()) {
            for (final Entry<EventStoreTimeIntervalEnum, Long> entry : eventStores.getCellsPutCount(statisticType)
                    .entrySet()) {
                list.add(new EventStoreAggregatorStatus("????", orderNo, "Cumulative cell put count",
                        entry.getKey().longName(), null, statisticType.name(), Long.toString(entry.getValue())));

            }
        }
    }

//    private void addBatchPutTaskPermitCount(final List<EventStoreAggregatorStatus> list, final int orderNo) {
//        list.add(new EventStoreAggregatorStatus("????", orderNo, "Available batch put task permits", "-", null, "-",
//                Integer.toString(eventStores.getAvailableBatchPutTaskPermits())));
//    }
}
