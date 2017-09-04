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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.configuration.StroomStatsStoreEntity;
import stroom.stats.service.config.Config;
import stroom.stats.configuration.common.Folder;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.service.startup.App;

public class HeadlessTestApp extends Application<Config> {

    Logger LOGGER = LoggerFactory.getLogger(App.class);
    private Injector injector = null;

    private final HibernateBundle<Config> hibernateBundle = new HibernateBundle<Config>(
            StroomStatsStoreEntity.class,
            Folder.class) {

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
        // This allows us to use templating in the YAML configuration.
        bootstrap.setConfigurationSourceProvider(new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(),
            new EnvironmentVariableSubstitutor(false)));

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
