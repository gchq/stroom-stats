package stroom.stats.mixins;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface HasHealthChecks {
    List<HasHealthCheck> checks();
}
