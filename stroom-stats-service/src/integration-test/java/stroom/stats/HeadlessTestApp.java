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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.config.Config;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.properties.StroomPropertyService;

public class HeadlessTestApp extends Application<Config> {

    Logger LOGGER = LoggerFactory.getLogger(App.class);
    private Injector injector = null;

    private final HibernateBundle<Config> hibernateBundle = new HibernateBundle<Config>(StatisticConfigurationEntity.class) {
        @Override
        public DataSourceFactory getDataSourceFactory(Config configuration) {
            return configuration.getDataSourceFactory();
        }
    };


    @Override
    public String getName() {
        return "headless-stroom-stats";
    }

    @Override
    public void initialize(Bootstrap<Config> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(Config config, Environment environment) {

        injector = Guice.createInjector(new StroomStatsServiceModule(config, hibernateBundle.getSessionFactory()));

        //log all the property service properties
        StroomPropertyService stroomPropertyService = injector.getInstance(StroomPropertyService.class);
        LOGGER.info("All property service properties:");
        stroomPropertyService.dumpAllProperties(LOGGER::info);
    }

    public Injector getInjector() {
        return injector;
    }

    public HibernateBundle<Config> getHibernateBundle() {
        return hibernateBundle;
    }
}
