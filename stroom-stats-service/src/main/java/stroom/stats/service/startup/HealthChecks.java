package stroom.stats.service.startup;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Injector;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.service.ServiceDiscoveryManager;
import stroom.stats.service.resources.ApiResource;
import stroom.stats.StatisticsAggregationService;
import stroom.stats.mixins.HasHealthCheck;
import stroom.stats.streams.StatisticsFlatMappingService;

import java.util.function.Supplier;

// Configuring HealthChecks is lengthy enough to deserve it's own file.
public class HealthChecks {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecks.class);

    static void register(Environment environment, Injector injector) {
        register(environment, "ApiResource",
                () -> injector.getInstance(ApiResource.class));

        ServiceDiscoveryManager serviceDiscoveryManager = injector.getInstance(ServiceDiscoveryManager.class);
        serviceDiscoveryManager.checks()
                .forEach(hasHealthCheck -> register(environment, hasHealthCheck));

        StatisticsFlatMappingService statisticsFlatMappingService = injector.getInstance(StatisticsFlatMappingService.class);
        register(environment, statisticsFlatMappingService);

        statisticsFlatMappingService.getHealthCheckProviders()
                .forEach(hasHealthCheck -> register(environment, hasHealthCheck));

        StatisticsAggregationService statisticsAggregationService = injector.getInstance(StatisticsAggregationService.class);
        register(environment, statisticsAggregationService);

        statisticsAggregationService.getHealthCheckProviders()
                .forEach(hasHealthCheck -> register(environment, hasHealthCheck));

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
}
