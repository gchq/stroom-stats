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
import stroom.stats.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.util.List;

@Singleton
public class ServiceDiscoveryManager {
    private final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);

    private Config config;

    // "When using Curator 2.x (Zookeeper 3.4.x) it's essential that service provider objects are cached by your
    // application and reused." - http://curator.apache.org/curator-x-discovery/
    private ServiceProvider<String> hbaseServiceProvider; // TODO: this instance isn't currently used - make it so or remove it
    private ServiceProvider<String> kafkaServiceProvider;
    private ServiceProvider<String> stroomDBServiceProvider; //TODO: this instance isn't currently used - make it so or remove ir

    private final ServiceDiscovery<String> serviceDiscovery;

    @Inject
    public ServiceDiscoveryManager(Config config) throws Exception {
        LOGGER.info("ServiceDiscoveryManager starting...");
        this.config = config;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZookeeperConfig().getQuorum(), retryPolicy);
        client.start();

        serviceDiscovery = ServiceDiscoveryBuilder
                .builder(String.class)
                .client(client)
                .basePath("stroom-services")
                .thisInstance(getThisServiceInstance(config))
                .build();
        serviceDiscovery.start();
    }

    @Inject
    private void startProviders() throws Exception {
        hbaseServiceProvider = serviceDiscovery.serviceProviderBuilder()
                .serviceName("hbase")
                .build();
        hbaseServiceProvider.start();

        kafkaServiceProvider = serviceDiscovery.serviceProviderBuilder()
                .serviceName("kafka")
                .build();
        kafkaServiceProvider.start();

        stroomDBServiceProvider = serviceDiscovery.serviceProviderBuilder()
                .serviceName("stroom-db")
                .build();
        stroomDBServiceProvider.start();
    }

    public ServiceInstance<String> getHBase() {
        try {
            return hbaseServiceProvider.getInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ServiceInstance<String> getKafka() {
        try {
            return kafkaServiceProvider.getInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ServiceInstance<String> getStroomDB() {
        try {
            return stroomDBServiceProvider.getInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
        List<ConnectorFactory> conectorFactories = serverFactory.getApplicationConnectors();
        if (Preconditions.checkNotNull(conectorFactories).size() != 1) {
            throw new RuntimeException(
                    String.format("Unexpected number of connectorFactories {}, check 'applicationConnectors' in the YAML config",
                            conectorFactories.size()));
        }

        HttpConnectorFactory connector = (HttpConnectorFactory) conectorFactories.get(0);
        if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
            port = connector.getPort();
        }
        return port;
    }
}
