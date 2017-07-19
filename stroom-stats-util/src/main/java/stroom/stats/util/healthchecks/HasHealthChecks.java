package stroom.stats.util.healthchecks;

import java.util.List;

@FunctionalInterface
public interface HasHealthChecks {
    List<HasHealthCheck> getHealthCheckProviders();
}
