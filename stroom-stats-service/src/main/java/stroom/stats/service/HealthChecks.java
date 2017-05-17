package stroom.stats.service;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Injector;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.service.resources.ApiResource;
import stroom.stats.StatisticsAggregationService;
import stroom.stats.mixins.HasHealthCheck;
import stroom.stats.streams.StatisticsFlatMappingService;

import java.util.function.Supplier;

public class HealthChecks {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecks.class);

    public static void register(Environment environment, Injector injector) {
        register(environment, "ApiResource",
                () -> injector.getInstance(ApiResource.class).getHealth());

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

    private static void register(final Environment environment,
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

    private static void register(final Environment environment,
                                     final HasHealthCheck hasHealthCheck) {

        LOGGER.info("Registering health check with name {}", hasHealthCheck.getName());

        environment.healthChecks().register(hasHealthCheck.getName(), new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return hasHealthCheck.check();
            }
        });
    }
}
