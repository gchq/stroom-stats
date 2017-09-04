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

package stroom.stats.service.startup;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.servlets.tasks.Task;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.configuration.StroomStatsStoreEntity;
import stroom.stats.datasource.DataSourceService;
import stroom.stats.service.ServiceDiscoverer;
import stroom.stats.service.ServiceDiscoveryManager;
import stroom.stats.service.auth.AuthenticationFilter;
import stroom.stats.service.auth.User;
import stroom.stats.service.resources.query.v1.QueryResource;
import stroom.stats.HBaseClient;
import stroom.stats.StroomStatsServiceModule;
import stroom.stats.service.config.Config;
import stroom.stats.configuration.common.Folder;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.tasks.StartProcessingTask;
import stroom.stats.tasks.StopProcessingTask;

import java.io.UnsupportedEncodingException;

public class App extends Application<Config> {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    public static final String APP_NAME = "stroom-stats";
    private Injector injector = null;

    private final HibernateBundle<Config> hibernateBundle = new HibernateBundle<Config>(
            StroomStatsStoreEntity.class,
            Folder.class) {

        @Override
        public DataSourceFactory getDataSourceFactory(Config configuration) {
            return configuration.getDataSourceFactory();
        }
    };

    public static void main(String[] args) throws Exception {
        new App().run(args);
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
    public void run(Config config, Environment environment) throws UnsupportedEncodingException {
        configureAuthentication(config, environment);

        injector = Guice.createInjector(new StroomStatsServiceModule(config, hibernateBundle.getSessionFactory()));
        injector.getInstance(ServiceDiscoveryManager.class);

        registerResources(environment);
        registerTasks(environment);
        HealthChecks.register(environment, injector);
        registerManagedObjects(environment);
    }

    @Override
    public String getName() {
        return APP_NAME;
    }

    public Injector getInjector() {
        return injector;
    }

    private void registerResources(final Environment environment) {
        LOGGER.info("Registering API");
        environment.jersey().register(new QueryResource(
                injector.getInstance(HBaseClient.class),
                injector.getInstance(DataSourceService.class),
                injector.getInstance(ServiceDiscoverer.class)));
    }

    private void registerTasks(final Environment environment) {
        registerTask(environment, StartProcessingTask.class);
        registerTask(environment, StopProcessingTask.class);
    }

    private <T extends Task> void registerTask(final Environment environment, Class<T> type) {
        LOGGER.info("Registering task with class {}", type.getName());
        T task = injector.getInstance(type);
        environment.admin().addTask(task);
    }

    private void registerManagedObjects(Environment environment) {
        registerManagedObject(environment, StatisticsIngestService.class);
        registerManagedObject(environment, ServiceDiscoveryManager.class);
        registerManagedObject(environment, ServiceDiscoverer.class);
    }

    private <T extends Managed> void registerManagedObject(final Environment environment, Class<T> type) {
        LOGGER.info("Registering managed object with class {}", type.getName());
        T managed = injector.getInstance(type);
        environment.lifecycle().manage(managed);
    }

    private static void configureAuthentication(Config config, Environment environment) {
        environment.jersey().register(new AuthDynamicFeature(AuthenticationFilter.get(config)));
        environment.jersey().register(new AuthValueFactoryProvider.Binder<>(User.class));
        environment.jersey().register(RolesAllowedDynamicFeature.class);
    }

}