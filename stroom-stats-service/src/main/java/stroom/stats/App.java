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

import com.codahale.metrics.health.HealthCheck;
import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
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

import java.io.UnsupportedEncodingException;
import java.util.List;

public class App extends Application<Config> {

    Logger LOGGER = LoggerFactory.getLogger(App.class);
    private Injector injector = null;

    private final HibernateBundle<Config> hibernateBundle = new HibernateBundle<Config>(StatisticConfigurationEntity.class) {
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

        // Register APIs
        environment.jersey().register(new ApiResource(injector.getInstance(HBaseClient.class)));

        registerHealthChecks(environment);
    }

    /**
     * This is an example method, showing how to load statistics.
     *
     * We're loading stats outside a Jersey call so we can't use @UnitOfWork to set up the session.
     * We need to get a session manually, as demonstrated here.
     */
    private Try<List<StatisticConfigurationEntity>> loadStats(){
        try (Session session = hibernateBundle.getSessionFactory().openSession()){
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
        return "stroom-stats";
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

    private void registerHealthChecks(Environment environment){

        environment.healthChecks().register("KafkaConsumerForStatisticsHealthCheck", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return injector.getInstance(KafkaConsumerForStatistics.class).getHealth();
            }
        });
        environment.healthChecks().register("ServiceDiscoveryManager_Kafka", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return injector.getInstance(ServiceDiscoveryManagerHealthCheck.class).getKafkaHealth();
            }
        });

        environment.healthChecks().register("ServiceDiscoveryManager_HBase", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return injector.getInstance(ServiceDiscoveryManagerHealthCheck.class).getHBaseHealth();
            }
        });

        environment.healthChecks().register("ServiceDiscoveryManager_StroomDB", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return injector.getInstance(ServiceDiscoveryManagerHealthCheck.class).getStroomDBHealth();
            }
        });
    }
}