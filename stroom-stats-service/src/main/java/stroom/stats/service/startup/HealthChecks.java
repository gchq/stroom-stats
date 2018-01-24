package stroom.stats.service.startup;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Injector;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.StatisticsAggregationService;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.logging.LogLevelInspector;
import stroom.stats.properties.StroomPropertyServiceHealthCheck;
import stroom.stats.service.ServiceDiscoveryRegistrar;
import stroom.stats.service.resources.query.v2.QueryResource;
import stroom.stats.streams.StatisticsFlatMappingService;
import stroom.stats.util.healthchecks.HasHealthCheck;

import java.util.List;
import java.util.function.Supplier;

// Configuring HealthChecks is lengthy enough to deserve it's own file.
public class HealthChecks {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecks.class);

    static void register(Environment environment, Injector injector) {
        register(environment, "QueryResource",
                () -> injector.getInstance(QueryResource.class));

        //TODO no services to discover at the moment
//        register(environment, injector.getInstance(ServiceDiscoverer.class));
        register(environment, injector.getInstance(ServiceDiscoveryRegistrar.class));

        StatisticsFlatMappingService statisticsFlatMappingService = injector.getInstance(StatisticsFlatMappingService.class);
        register(environment, statisticsFlatMappingService);
//
        StatisticsAggregationService statisticsAggregationService = injector.getInstance(StatisticsAggregationService.class);
        register(environment, statisticsAggregationService);

        register(environment, LogLevelInspector.INSTANCE);
        register(environment, injector.getInstance(StroomPropertyServiceHealthCheck.class));
        register(environment, injector.getInstance(HBaseConnection.class));
    }

    /**
     * For use when the {@link HasHealthCheck} instance is not known at registration time, or may change
     */
    private static void register(final Environment environment,
                                 final String name,
                                 final Supplier<HasHealthCheck> hasHealthCheckSupplier) {

        LOGGER.info("Registering health check with name {}", name);

        environment.healthChecks().register(name, new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return hasHealthCheckSupplier.get().getHealth();
            }
        });
    }

    private static void register(final Environment environment,
                                 final HasHealthCheck hasHealthCheck) {

        LOGGER.info("Registering health check with name {}", hasHealthCheck.getName());

        environment.healthChecks().register(hasHealthCheck.getName(), hasHealthCheck.getHealthCheck());
    }

    private static void register(final Environment environment,
                                 final String healthCheckName,
                                 final List<HasHealthCheck> healthCheckProviders) {

        LOGGER.info("Registering aggregate health check with name {}", healthCheckName);

        environment.healthChecks().register(healthCheckName,
                HasHealthCheck.getAggregateHealthCheck(healthCheckName, healthCheckProviders).getHealthCheck());
    }

}
