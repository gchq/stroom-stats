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

package stroom.stats.service;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.HBaseClient;
import stroom.stats.service.config.Config;
import stroom.stats.mixins.HasHealthCheck;
import stroom.stats.mixins.HasHealthChecks;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Singleton
public class ServiceDiscoveryManager implements HasHealthChecks {
    private final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);

    private Config config;

    private final ServiceDiscovery<String> serviceDiscovery;

    // "When using Curator 2.x (Zookeeper 3.4.x) it's essential that service provider objects are cached by your
    // application and reused." - http://curator.apache.org/curator-x-discovery/
    private Map<ExternalServices, ServiceProvider<String>> serviceProviders = new HashMap<>();

    @Inject
    public ServiceDiscoveryManager(Config config) throws Exception {
        LOGGER.info("ServiceDiscoveryManager starting...");
        this.config = config;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZookeeperConfig().getQuorum(), retryPolicy);
        client.start();

        // First register this service
        serviceDiscovery = ServiceDiscoveryBuilder
                .builder(String.class)
                .client(client)
                .basePath("stroom-services")
                .thisInstance(getThisServiceInstance(config))
                .build();
        serviceDiscovery.start();

        // Then register services this service depends on
        Arrays.stream(ExternalServices.values()).forEach(externalService ->
            serviceProviders.put(externalService, createProvider(externalService.getName())));

        client.close();
    }

    private ServiceProvider<String> createProvider(String name){
        ServiceProvider<String> provider = serviceDiscovery.serviceProviderBuilder()
                .serviceName(name)
                .build();
        try {
            provider.start();
        } catch (Exception e) {
            LOGGER.error("Unable to start service provider for " + name + "!", e);
            e.printStackTrace();
        }

        return provider;
    }

    public ServiceInstance<String> get(ExternalServices externalService){
        try {
            return serviceProviders.get(externalService).getInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getAddress(ExternalServices externalService) {
        ServiceInstance<String> instance = get(externalService);
        return instance == null ?
                Optional.empty() :
                instance.getAddress() == null ?
                        Optional.empty() :
                        Optional.of(instance.getAddress());
    }

    //TODO registering needs to go somewhere else
    private static ServiceInstance<String> getThisServiceInstance(Config config) throws Exception {
        String ipAddress = InetAddress.getLocalHost().getHostAddress();
        int port = getPort(config);

        ServiceInstance<String> thisInstance = ServiceInstance.<String>builder()
                .name("stats")
                .address(ipAddress)
                .port(port)
                .build();
        return thisInstance;
    }

    private static int getPort(Config config){
        int port = 0;
        DefaultServerFactory serverFactory = (DefaultServerFactory) config.getServerFactory();
        List<ConnectorFactory> connectorFactories = serverFactory.getApplicationConnectors();
        if (Preconditions.checkNotNull(connectorFactories).size() != 1) {
            throw new RuntimeException(
                    String.format("Unexpected number of connectorFactories %s, check 'applicationConnectors' in the YAML config",
                            connectorFactories.size()));
        }

        HttpConnectorFactory connector = (HttpConnectorFactory) connectorFactories.get(0);
        if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
            port = connector.getPort();
        }
        return port;
    }

    @Override
    public List<HasHealthCheck> checks() {
        List<HasHealthCheck> checks = new ArrayList<>();
        for (ExternalServices externalService : ExternalServices.values()) {
            checks.add(
                    new HasHealthCheck() {
                        @Override
                        public HealthCheck.Result check() {
                            return checkThis(get(externalService), externalService.getName());
                        }

                        @Override
                        public String getName() {
                            return "ServiceDiscoveryManager_" + externalService.getName();
                        }
                    }
            );
        }
        return checks;
    }

    private HealthCheck.Result checkThis(final ServiceInstance<String> serviceInstance,
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
