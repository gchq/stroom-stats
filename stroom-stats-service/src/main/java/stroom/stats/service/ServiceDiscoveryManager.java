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

import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.properties.ServiceDiscoveryCuratorFramework;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Singleton
public class ServiceDiscoveryManager implements Managed {
    private final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);

//    private final ServiceDiscovery<String> serviceDiscovery;


//    private Map<ExternalService, ServiceProvider<String>> serviceProviders = new HashMap<>();
//    private CuratorFramework curatorFramework;


    private final AtomicReference<CuratorFramework> curatorFrameworkRef = new AtomicReference<>();
    private final AtomicReference<ServiceDiscovery<String>> serviceDiscoveryRef = new AtomicReference<>();
    private final List<Consumer<ServiceDiscovery<String>>> curatorStartupListeners = new ArrayList<>();
    private final Deque<Closeable> closeables = new LinkedList<>();

    @Inject
    public ServiceDiscoveryManager(final @ServiceDiscoveryCuratorFramework CuratorFramework curatorFramework)
            throws Exception {

        LOGGER.info("ServiceDiscoveryManager starting...");
        this.curatorFrameworkRef.set(curatorFramework);


//        // First register this service
//        serviceDiscovery = ServiceDiscoveryBuilder
//                .builder(String.class)
//                .client(curatorFramework)
//                .basePath("/")
//                .thisInstance(getThisServiceInstance(config))
//                .build();
//        serviceDiscovery.start();

        // Then register services this service depends on
//        Arrays.stream(ExternalService.values()).forEach(externalService ->
//            serviceProviders.put(externalService, createProvider(externalService.get())));
    }

    @Override
    public void start() throws Exception {
        startServiceDiscovery();
        notifyListeners();
    }

    @Override
    public void stop() {
        closeables.forEach(closeable -> {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    LOGGER.error("Error while closing {}", closeable.getClass().getCanonicalName(), e);
                }
            }
        });
    }

    public void registerStartupListener(final Consumer<ServiceDiscovery<String>> listener) {
        if (serviceDiscoveryRef.get() != null) {
            //already started so call the listener now
            listener.accept(serviceDiscoveryRef.get());
        } else {
            curatorStartupListeners.add(Preconditions.checkNotNull(listener));
        }
    }

    private void notifyListeners() {

        if (serviceDiscoveryRef.get() != null) {
            //now notify all listeners
            curatorStartupListeners.forEach(listener -> listener.accept(serviceDiscoveryRef.get()));
        } else {
            LOGGER.error("Unable to notify listeners of serviceDiscovery starting, serviceDiscovery is null");
        }
    }

    private void startServiceDiscovery() {

        //already chrooted so use /
        String basePath = "/";

        ServiceDiscovery<String> serviceDiscovery = ServiceDiscoveryBuilder
                .builder(String.class)
                .client(Preconditions.checkNotNull(curatorFrameworkRef.get(), "curatorFramework should not be null at this point"))
                .basePath(basePath)
                .build();

        try {
            serviceDiscovery.start();
            //push to the top of the queue to ensure this gets closed before the curator framework else it
            //won't work as it has no client
            closeables.push(serviceDiscovery);
            boolean wasSet = serviceDiscoveryRef.compareAndSet(null, serviceDiscovery);
            if (!wasSet) {
                LOGGER.error("Attempt to set serviceDiscoveryRef when already set");
            } else {
                LOGGER.info("Successfully started ServiceDiscovery on path " + basePath);
            }

        } catch (Exception e) {
            throw new RuntimeException(String.format("Error starting ServiceDiscovery with base path %s", basePath), e);
        }
    }

//    private ServiceProvider<String> createProvider(String name){
//        ServiceProvider<String> provider = serviceDiscovery.serviceProviderBuilder()
//                .serviceName(name)
//                .build();
//        try {
//            provider.start();
//        } catch (Exception e) {
//            LOGGER.error("Unable to start service provider for " + name + "!", e);
//            e.printStackTrace();
//        }
//
//        return provider;
//    }

//    public ServiceInstance<String> get(ExternalService externalService){
//        try {
//            return serviceProviders.get(externalService).getInstance();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

//    public Optional<String> getAddress(ExternalService externalService) {
//        ServiceInstance<String> instance = get(externalService);
//        return instance == null ?
//                Optional.empty() :
//                instance.getAddress() == null ?
//                        Optional.empty() :
//                        Optional.of(instance.getAddress());
//    }


//    @Override
//    public List<HasHealthCheck> getHealthCheckProviders() {
//        List<HasHealthCheck> checks = new ArrayList<>();
//        for (ExternalService externalService : ExternalService.values()) {
//            checks.add(
//                    new HasHealthCheck() {
//                        @Override
//                        public HealthCheck.Result getHealth() {
//                            return checkThis(get(externalService), externalService.getName());
//                        }
//
//                        @Override
//                        public String getName() {
//                            return "ServiceDiscoveryManager_" + externalService.getName();
//                        }
//                    }
//            );
//        }
//        return checks;
//    }

//    private HealthCheck.Result checkThis(final ServiceInstance<String> serviceInstance,
//                                     final String serviceInstanceName) {
//        if (serviceInstance == null) {
//            return HealthCheck.Result.unhealthy(String.format(
//                    "There are no registered instances of the '%s' service for me to use!",
//                    serviceInstanceName));
//        } else {
//            return HealthCheck.Result.healthy(String.format(
//                    "Found an instance of the '%s' service for me to use at %s.",
//                    serviceInstanceName,
//                    serviceInstance.getAddress()));
//        }
//    }

}
