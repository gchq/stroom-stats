package stroom.stats.properties;

import com.codahale.metrics.health.HealthCheck;
import stroom.stats.util.healthchecks.HasHealthCheck;

import javax.inject.Inject;
import java.util.TreeMap;

public class StroomPropertyServiceHealthCheck implements HasHealthCheck {

    private final StroomPropertyService stroomPropertyService;

    @Inject
    public StroomPropertyServiceHealthCheck(final StroomPropertyService stroomPropertyService) {
        this.stroomPropertyService = stroomPropertyService;
    }

    @Override
    public HealthCheck.Result getHealth() {
        if (stroomPropertyService == null) {
            return HealthCheck.Result.unhealthy("stroomPropertyService has not been initialised");
        } else {

            try {
                //use a treeMap so the props are sorted on output
                return HealthCheck.Result.builder()
                        .withMessage("Available")
                        .withDetail("properties", new TreeMap<>(stroomPropertyService.getAllProperties()))
                        .build();

            } catch (Exception e) {
                return HealthCheck.Result.unhealthy(e);
            }
        }
    }
}
