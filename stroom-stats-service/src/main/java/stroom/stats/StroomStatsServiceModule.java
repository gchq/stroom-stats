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
import com.google.inject.Provides;
import org.apache.curator.framework.CuratorFramework;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.hibernate.SessionFactory;
import stroom.stats.api.StatisticsService;
import stroom.stats.config.Config;
import stroom.stats.configuration.StatisticConfigurationEntityDAO;
import stroom.stats.configuration.StatisticConfigurationEntityDAOImpl;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticConfigurationServiceImpl;
import stroom.stats.properties.CuratorFrameworkProvider;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.properties.StroomPropertyServiceImpl;
import stroom.stats.streams.KafkaStreamService;
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
        bind(CuratorFramework.class).toProvider(CuratorFrameworkProvider.class).asEagerSingleton();
        bind(HBaseClient.class);
        bind(KafkaStreamService.class).asEagerSingleton();
        bind(ServiceDiscoveryManager.class);
        bind(ServiceDiscoveryManagerHealthCheck.class);
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
