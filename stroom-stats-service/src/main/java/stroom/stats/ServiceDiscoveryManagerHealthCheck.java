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
import org.apache.curator.x.discovery.ServiceInstance;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ServiceDiscoveryManagerHealthCheck {

    private ServiceDiscoveryManager serviceDiscoveryManager;

    @Inject
    public ServiceDiscoveryManagerHealthCheck(ServiceDiscoveryManager serviceDiscoveryManager) {
        this.serviceDiscoveryManager = serviceDiscoveryManager;
    }

    public Map<ExternalServices, Supplier<HealthCheck.Result>> getChecks(){
        Map<ExternalServices, Supplier<HealthCheck.Result>> checks = new HashMap<>();
        Arrays.stream(ExternalServices.values()).forEach(externalService ->
                checks.put(
                        externalService,
                        () -> check(serviceDiscoveryManager.get(externalService), externalService.getName())
                )
        );
        return checks;
    }

    private HealthCheck.Result check(final ServiceInstance<String> serviceInstance,
                                     final String serviceInstanceName) {

        if (serviceInstance == null) {
            return HealthCheck.Result.unhealthy(String.format(
                    "There are no registered instances of the '%s' service for me to use!",
                    serviceInstanceName));
        } else {
            return HealthCheck.Result.healthy(String.format(
                    "Found an instance of the '%s' service for me to use at %s.",
                    serviceInstanceName,
                    serviceInstance.getAddress()));
        }
    }


}
