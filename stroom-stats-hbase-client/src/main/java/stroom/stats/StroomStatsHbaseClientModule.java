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

package stroom.stats;

import com.google.inject.AbstractModule;
import org.ehcache.CacheManager;
import stroom.stats.api.StatisticsService;
import stroom.stats.cache.CacheConfigurationService;
import stroom.stats.cache.CacheConfigurationServiceImpl;
import stroom.stats.cache.CacheFactory;
import stroom.stats.cache.CacheFactoryImpl;
import stroom.stats.cluster.ClusterLockService;
import stroom.stats.cluster.ClusterLockServiceImpl;
import stroom.stats.common.StatisticConfigurationValidator;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.HBaseStatisticsService;
import stroom.stats.hbase.RowKeyCache;
import stroom.stats.hbase.RowKeyCacheImpl;
import stroom.stats.hbase.aggregator.EventStoresPutAggregator;
import stroom.stats.hbase.aggregator.EventStoresPutAggregatorImpl;
import stroom.stats.hbase.aggregator.EventStoresThreadFlushAggregator;
import stroom.stats.hbase.aggregator.EventStoresThreadFlushAggregatorImpl;
import stroom.stats.hbase.aggregator.InMemoryEventStoreIdPool;
import stroom.stats.hbase.aggregator.InMemoryEventStoreIdPoolImpl;
import stroom.stats.hbase.table.*;
import stroom.stats.hbase.uid.UniqueIdGenerator;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCacheImpl;
import stroom.stats.hbase.uid.UniqueIdProvider;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.server.common.StatisticConfigurationValidatorImpl;
import stroom.stats.task.TaskManagerImpl;
import stroom.stats.task.api.TaskManager;

public class StroomStatsHbaseClientModule extends AbstractModule {

    @Override
    protected void configure() {
        //bindings that parent modules must provide
        requireBinding(CacheManager.class);
        requireBinding(StatisticConfigurationService.class);
        requireBinding(StroomPropertyService.class);

        //simple binds
        bind(CacheConfigurationService.class).to(CacheConfigurationServiceImpl.class);
        bind(CacheFactory.class).to(CacheFactoryImpl.class);
        bind(ClusterLockService.class).to(ClusterLockServiceImpl.class);
        bind(EventStoresPutAggregator.class).to(EventStoresPutAggregatorImpl.class);
        bind(EventStoresThreadFlushAggregator.class).to(EventStoresThreadFlushAggregatorImpl.class);
        bind(InMemoryEventStoreIdPool.class).to(InMemoryEventStoreIdPoolImpl.class);
        bind(RowKeyCache.class).to(RowKeyCacheImpl.class);
        bind(StatisticConfigurationValidator.class).to(StatisticConfigurationValidatorImpl.class);
        bind(StatisticsService.class).to(HBaseStatisticsService.class);
        bind(TaskManager.class).to(TaskManagerImpl.class);
        bind(UniqueIdCache.class).to(UniqueIdCacheImpl.class);
        bind(UniqueIdForwardMapTable.class).to(HBaseUniqueIdForwardMapTable.class);
        bind(UniqueIdReverseMapTable.class).to(HBaseUniqueIdReverseMapTable.class);

        //providers
        bind(UniqueIdGenerator.class).toProvider(UniqueIdProvider.class).asEagerSingleton();

        //singletons
        bind(EventStoreTableFactory.class).to(HBaseEventStoreTableFactory.class).asEagerSingleton();
    }

}
