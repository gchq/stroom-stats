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

package stroom.stats;

import com.google.inject.AbstractModule;
import org.apache.curator.framework.CuratorFramework;
import org.mockito.Mockito;
import stroom.stats.api.StatisticsService;
import stroom.stats.configuration.MockStatisticConfigurationService;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.table.TableFactory;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.MockStroomPropertyService;
import stroom.stats.properties.StroomPropertyService;

public class StroomStatsEmbeddedOverrideModule extends AbstractModule {

    private final TableFactory mockTableFactory = Mockito.mock(TableFactory.class);
    private final StatisticsService mockStatisticsService = Mockito.mock(StatisticsService.class, Mockito.withSettings().verboseLogging());
    private final CuratorFramework mockCuratorFramework = Mockito.mock(CuratorFramework.class);

    private final MockStatisticConfigurationService mockStatisticConfigurationService = new MockStatisticConfigurationService();

    private final MockStroomPropertyService mockStroomPropertyService;

    public StroomStatsEmbeddedOverrideModule(final MockStroomPropertyService mockStroomPropertyService) {
        this.mockStroomPropertyService = mockStroomPropertyService;
    }

    @Override
    protected void configure() {
        //singletons
        bind(UniqueIdCache.class).to(MockUniqueIdCache.class).asEagerSingleton();

        //TODO For the moment, just a mock that does nothing. In furture will either need
        //to capture calls made to it for asserts or be replaced with the HBaseTestingUtility
        //if that can be made to work with the hbase-shaded-client
        bind(TableFactory.class).toInstance(mockTableFactory);


        //TODO need to replace this with a proper mock, or expose this mock somehow to allow capturing of input
        //to it.  Think the above TabFac binding is not needed as it will never get taht far
        bind(StatisticsService.class).toInstance(mockStatisticsService);


        //Don't need curator framework as we are using a mocked StroomPropertyService
        bind(CuratorFramework.class).toInstance(mockCuratorFramework);

        bind(StroomPropertyService.class).toInstance(mockStroomPropertyService);

        bind(StatisticConfigurationService.class).toInstance(mockStatisticConfigurationService);
    }

    public StatisticsService getMockStatisticsService() {
        return mockStatisticsService;
    }

    public MockStatisticConfigurationService getMockStatisticConfigurationService() {
        return mockStatisticConfigurationService;
    }
}
