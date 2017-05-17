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

import com.codahale.metrics.health.HealthCheck;
import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.servlets.tasks.Task;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import javaslang.control.Try;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.hibernate.Session;
import org.hibernate.context.internal.ManagedSessionContext;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.config.Config;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticConfigurationEntityDAOImpl;
import stroom.stats.configuration.common.Folder;
import stroom.stats.mixins.HasHealthCheck;
import stroom.stats.streams.StatisticsFlatMappingService;
import stroom.stats.streams.StatisticsIngestService;
import stroom.stats.tasks.StartProcessingTask;
import stroom.stats.tasks.StopProcessingTask;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.function.Supplier;

public class App extends Application<Config> {

    public static final String APP_NAME = "stroom-stats";
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private Injector injector = null;

    private final HibernateBundle<Config> hibernateBundle = new HibernateBundle<Config>(
            StatisticConfigurationEntity.class,
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
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(Config config, Environment environment) throws UnsupportedEncodingException {

        configureAuthentication(config, environment);

        // Bootstrap Guice
        injector = Guice.createInjector(new StroomStatsServiceModule(config, hibernateBundle.getSessionFactory()));
        // There are no dependencies on KafkaConsumerForStatistics so we need to make sure it starts by calling getInstance(...)
        injector.getInstance(ServiceDiscoveryManager.class);


        loadStats()
                .onFailure(e -> LOGGER.error("Unable to retrieve statistics: {}", e))
                .onSuccess(stats -> LOGGER.info("Retrieved {} statistics, but not doing anything", stats.size()));

        registerAPIs(environment);
        registerTasks(environment);
        registerHealthChecks(environment);
        registerManagedObjects(environment);
    }

    private void registerAPIs(final Environment environment) {

        LOGGER.info("Registering API");
        environment.jersey().register(new ApiResource(
                injector.getInstance(HBaseClient.class),
                injector.getInstance(ServiceDiscoveryManager.class)));
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

    private void registerHealthChecks(Environment environment) {
        registerHealthCheck(environment, "ApiResource",
                () -> injector.getInstance(ApiResource.class).getHealth());

        ServiceDiscoveryManagerHealthCheck serviceDiscoveryManagerHealthCheck = injector.getInstance(ServiceDiscoveryManagerHealthCheck.class);
        serviceDiscoveryManagerHealthCheck.getChecks().entrySet().forEach(check -> {
            environment.healthChecks().register(
                    "ServiceDiscoveryManager_" + check.getKey().getName(),
                    new HealthCheck() {
                        @Override
                        protected Result check() throws Exception {
                            return check.getValue().get();
                        }
                    });
        });

        StatisticsFlatMappingService statisticsFlatMappingService = injector.getInstance(StatisticsFlatMappingService.class);
        registerHealthCheck(environment, statisticsFlatMappingService);

        statisticsFlatMappingService.getHealthCheckProviders()
                .forEach(hasHealthCheck -> registerHealthCheck(environment, hasHealthCheck));

        StatisticsAggregationService statisticsAggregationService = injector.getInstance(StatisticsAggregationService.class);
        registerHealthCheck(environment, statisticsAggregationService);

        statisticsAggregationService.getHealthCheckProviders()
                .forEach(hasHealthCheck -> registerHealthCheck(environment, hasHealthCheck));

    }

    private void registerHealthCheck(final Environment environment,
                                     final String name,
                                     final Supplier<HealthCheck.Result> healthCheckResultSupplier) {

        LOGGER.info("Registering health check with name {}", name);

        environment.healthChecks().register(name, new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return healthCheckResultSupplier.get();
            }
        });
    }

    private void registerHealthCheck(final Environment environment,
                                     final HasHealthCheck hasHealthCheck) {

        LOGGER.info("Registering health check with name {}", hasHealthCheck.getName());

        environment.healthChecks().register(hasHealthCheck.getName(), new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return hasHealthCheck.check();
            }
        });
    }

    private void registerManagedObjects(Environment environment) {
        registerManagedObject(environment, StatisticsIngestService.class);
    }

    private <T extends Managed> void registerManagedObject(final Environment environment, Class<T> type) {

        LOGGER.info("Registering managed object with class {}", type.getName());
        T managed = injector.getInstance(type);
        environment.lifecycle().manage(managed);
    }

    /**
     * This is an example method, showing how to load statistics.
     * <p>
     * We're loading stats outside a Jersey call so we can't use @UnitOfWork to set up the session.
     * We need to get a session manually, as demonstrated here.
     */
    private Try<List<StatisticConfigurationEntity>> loadStats() {
        try (Session session = hibernateBundle.getSessionFactory().openSession()) {
            ManagedSessionContext.bind(session);
            session.beginTransaction();
            StatisticConfigurationEntityDAOImpl dao = injector.getInstance(StatisticConfigurationEntityDAOImpl.class);
            List<StatisticConfigurationEntity> allStats = dao.loadAll();
            return Try.success(allStats);
        } catch (Exception e) {
            return Try.failure(e);
        }
    }

    @Override
    public String getName() {
        return APP_NAME;
    }

    public Injector getInjector() {
        return injector;
    }

    private static void configureAuthentication(Config config, Environment environment) throws UnsupportedEncodingException {

        final JwtConsumer consumer = new JwtConsumerBuilder()
                .setAllowedClockSkewInSeconds(30) // allow some leeway in validating time based claims to account for clock skew
                .setRequireExpirationTime() // the JWT must have an expiration time
                .setRequireSubject() // the JWT must have a subject claim
                .setVerificationKey(new HmacKey(config.getJwtTokenSecret())) // verify the signature with the public key
                .setRelaxVerificationKeyValidation() // relaxes key length requirement
                .setExpectedIssuer("stroom")
                .build();

        environment.jersey().register(new AuthDynamicFeature(
                new JwtAuthFilter.Builder<User>()
                        .setJwtConsumer(consumer)
                        .setRealm("realm")
                        .setPrefix("Bearer")
                        .setAuthenticator(new UserAuthenticator())
                        .buildAuthFilter()));

        environment.jersey().register(new AuthValueFactoryProvider.Binder<>(User.class));
        environment.jersey().register(RolesAllowedDynamicFeature.class);
    }
}