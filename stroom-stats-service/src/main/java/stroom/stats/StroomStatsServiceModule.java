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
import com.google.inject.Provides;
import org.apache.curator.framework.CuratorFramework;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.hibernate.SessionFactory;
import stroom.stats.api.StatisticsService;
import stroom.stats.properties.ServiceDiscoveryCuratorFrameworkProvider;
import stroom.stats.properties.ServiceDiscoveryCuratorFramework;
import stroom.stats.properties.StatsCuratorFramework;
import stroom.stats.service.config.Config;
import stroom.stats.configuration.StatisticConfigurationEntityDAO;
import stroom.stats.configuration.StatisticConfigurationEntityDAOImpl;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticConfigurationServiceImpl;
import stroom.stats.properties.StatsCuratorFrameworkProvider;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.properties.StroomPropertyServiceImpl;
import stroom.stats.service.ServiceDiscoveryManager;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.xml.StatisticsMarshaller;

public class StroomStatsServiceModule extends AbstractModule {

    private Config config;
    private SessionFactory sessionFactory;

    public StroomStatsServiceModule(Config config, SessionFactory sessionFactory){
        this.config = config;
        this.sessionFactory = sessionFactory;
    }

    @Override
    protected void configure() {
        install(new StroomStatsHbaseClientModule());

        //Should be provided by StroomStatsHBaseClientModule
        requireBinding(StatisticsService.class);

        //Singleton as this holds the details of all the caches
        bind(CacheManager.class)
                .toProvider(() -> CacheManagerBuilder.newCacheManagerBuilder().build(true))
                .asEagerSingleton();
        //Singleton as this holds the connection to Zookeeper
        bind(CuratorFramework.class).annotatedWith(StatsCuratorFramework.class).toProvider(StatsCuratorFrameworkProvider.class).asEagerSingleton();
        bind(CuratorFramework.class).annotatedWith(ServiceDiscoveryCuratorFramework.class).toProvider(ServiceDiscoveryCuratorFrameworkProvider.class).asEagerSingleton();
        bind(HBaseClient.class);
        bind(StatisticsIngestService.class).asEagerSingleton();
        bind(ServiceDiscoveryManager.class);
        bind(SessionFactory.class).toInstance(sessionFactory);
        bind(StatisticConfigurationEntityDAO.class).to(StatisticConfigurationEntityDAOImpl.class);
        bind(StatisticConfigurationService.class).to(StatisticConfigurationServiceImpl.class);
        //singleton to avoid cost of repeatedly creating JAXBContext
        bind(StatisticsMarshaller.class).asEagerSingleton();
        //Singleton as it holds a cache of the properties
        bind(StroomPropertyService.class).to(StroomPropertyServiceImpl.class).asEagerSingleton();
    }

    @Provides
    public Config getConfig() {
        return config;
    }
}
