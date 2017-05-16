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

public class ServiceDiscoveryManagerHealthCheck {

    private ServiceDiscoveryManager serviceDiscoveryManager;

    @Inject
    public ServiceDiscoveryManagerHealthCheck(ServiceDiscoveryManager serviceDiscoveryManager) {

        this.serviceDiscoveryManager = serviceDiscoveryManager;
    }

    public HealthCheck.Result getKafkaHealth() {
        return check(serviceDiscoveryManager.getKafka(), "Kafka");

    }

    public HealthCheck.Result getHBaseHealth() {
        return check(serviceDiscoveryManager.getHBase(), "HBase");

    }

    public HealthCheck.Result getStroomDBHealth() {
        return check(serviceDiscoveryManager.getStroomDB(), "stroom-db");
    }

    public HealthCheck.Result getStroomHealth() {
        return check(serviceDiscoveryManager.getStroom(), "stroom");
    }

    private HealthCheck.Result check(final ServiceInstance<String> serviceInstance,
                                     final String serviceInstanceName) {

        if (serviceInstance == null) {
            return HealthCheck.Result.unhealthy(String.format(
                    "There are no registered instances of %s for me to use!",
                    serviceInstanceName));
        } else {
            return HealthCheck.Result.healthy(String.format(
                    "Found a %s instance for me to use at %s:%s.",
                    serviceInstanceName,
                    serviceInstance.getAddress(),
                    serviceInstance.getPort()));
        }
    }


}
