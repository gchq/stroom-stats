package stroom.stats.mixins;

import java.util.List;

@FunctionalInterface
public interface HasHealthChecks {
    List<HasHealthCheck> getHealthCheckProviders();
}
