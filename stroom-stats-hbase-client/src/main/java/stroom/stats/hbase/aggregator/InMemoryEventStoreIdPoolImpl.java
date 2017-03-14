

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
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.properties.StroomPropertyService;

import javax.inject.Inject;
import java.util.EnumMap;
import java.util.Map;

/**
 * Wrapper bean to hold two IdPool objects for use in controlling the flow of
 * events into the in memory event stores.
 */
public class InMemoryEventStoreIdPoolImpl implements InMemoryEventStoreIdPool {
    private final Map<StatisticType, IdPool> idPoolMap = new EnumMap<>(StatisticType.class);

    @Inject
    public InMemoryEventStoreIdPoolImpl(final StroomPropertyService propertyService) {
        for (final StatisticType statisticType : StatisticType.values()) {
            final int idPoolSize = propertyService.getIntPropertyOrThrow(HBaseStatisticConstants.THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX
                            + statisticType.name().toLowerCase());

            idPoolMap.put(statisticType, new IdPool(idPoolSize));
        }
    }

    @Override
    public int getId(final StatisticType statisticType) throws InterruptedException {
        return idPoolMap.get(statisticType).getId();
    }

    @Override
    public void returnId(final StatisticType statisticType, final int id) {
        idPoolMap.get(statisticType).returnId(id);
    }

    @Override
    public void disablePool(final StatisticType statisticType) {
        idPoolMap.get(statisticType).disablePool();
    }

    @Override
    public void enablePool(final StatisticType statisticType) {
        idPoolMap.get(statisticType).enablePool();
    }

    @Override
    public int getPoolSize(final StatisticType statisticType) {
        return idPoolMap.get(statisticType).getPoolSize();
    }

    @Override
    public boolean getEnabledState(final StatisticType statisticType) {
        return idPoolMap.get(statisticType).getEnabledState();
    }

    @Override
    public int getAvailablePermitCount(final StatisticType statisticType) {
        return idPoolMap.get(statisticType).getAvailablePermitCount();
    }
}
