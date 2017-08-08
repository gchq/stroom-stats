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
import org.apache.curator.framework.CuratorFramework;
import org.mockito.Mockito;
import stroom.stats.api.StatisticsService;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.table.EventStoreTableFactory;
import stroom.stats.hbase.table.HBaseEventStoreTableFactory;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdGenerator;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.properties.StroomPropertyService;

import java.util.Optional;

public class StroomStatsEmbeddedOverrideModule extends AbstractModule {

    private final EventStoreTableFactory mockEventStoreTableFactory = Mockito.mock(EventStoreTableFactory.class);
    private final StatisticsService mockStatisticsService;
    private final CuratorFramework mockCuratorFramework = Mockito.mock(CuratorFramework.class);

    private final MockStatisticConfigurationService mockStatisticConfigurationService = new MockStatisticConfigurationService();

    private final MockStroomPropertyService mockStroomPropertyService;

    public StroomStatsEmbeddedOverrideModule(final MockStroomPropertyService mockStroomPropertyService, final Optional<StatisticsService> mockStatisticsService) {
        this.mockStroomPropertyService = mockStroomPropertyService;
        this.mockStatisticsService = mockStatisticsService.orElseGet(() -> Mockito.mock(StatisticsService.class));


    }

    @Override
    protected void configure() {
        //singletons
        bind(UniqueIdCache.class).to(MockUniqueIdCache.class).asEagerSingleton();

        //TODO For the moment, just a mock that does nothing. In furture will either need
        //to capture calls made to it for asserts or be replaced with the HBaseTestingUtility
        //if that can be made to work with the hbase-shaded-client
        bind(EventStoreTableFactory.class).toInstance(mockEventStoreTableFactory);


        //TODO need to replace this with a proper mock, or expose this mock somehow to allow capturing of input
        //to it.  Think the above TabFac binding is not needed as it will never get taht far
        bind(StatisticsService.class).toInstance(mockStatisticsService);


        //Don't need curator framework as we are using a mocked StroomPropertyService
        bind(CuratorFramework.class).toInstance(mockCuratorFramework);

        bind(StroomPropertyService.class).toInstance(mockStroomPropertyService);

        bind(StatisticConfigurationService.class).toInstance(mockStatisticConfigurationService);

        //don't want an actual hbase so just mock the classes
        bindToMock(HBaseConnection.class);
        bindToMock(HBaseEventStoreTableFactory.class);
        bindToMock(UniqueIdGenerator.class);
    }

    public MockStatisticConfigurationService getMockStatisticConfigurationService() {
        return mockStatisticConfigurationService;
    }

    private <T> T bindToMock(Class<T> clazz) {
        T mock = Mockito.mock(clazz);
        bind(clazz).toInstance(mock);
        return mock;
    }
}
