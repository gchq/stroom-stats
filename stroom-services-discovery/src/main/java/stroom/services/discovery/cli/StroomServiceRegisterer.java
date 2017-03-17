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

package stroom.services.discovery.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StroomServiceRegisterer {
    private final Logger LOGGER = LoggerFactory.getLogger(StroomServiceRegisterer.class);

    @Parameter(names={"--name", "-n"}, description="The name of this service", required=true)
    String name;

    @Parameter(names={"--ipAddress", "-ip"}, description="The ipAddress you want to advertise this service on", required=true)
    String ipAddress;

    @Parameter(names={"--port", "-p"}, description="The port this service is running on", required=true)
    int port;

    @Parameter(names={"--zookeeper", "-zk"}, description="The address of zookeeper", required=true)
    String zookeeper;



    public static void main(String[] args) throws Exception {
        StroomServiceRegisterer stroomServiceRegisterer = new StroomServiceRegisterer();
        new JCommander(stroomServiceRegisterer, args);
        stroomServiceRegisterer.createServiceDiscovery();
    }

    private void createServiceDiscovery() {
        LOGGER.info("Starting Curator client using Zookeeper at '{}'...", zookeeper);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
        client.start();

        try {
            LOGGER.info("Setting up instance for '{}' service, running on '{}:{}'...", name, ipAddress, port);
            ServiceInstance<String> instance = ServiceInstance.<String>builder()
                    .serviceType(ServiceType.PERMANENT)
                    .name(name)
                    .address(ipAddress)
                    .port(port)
                    .build();

            ServiceDiscovery serviceDiscovery = ServiceDiscoveryBuilder
                    .builder(String.class)
                    .client(client)
                    .basePath("stroom-services")
                    .thisInstance(instance)
                    .build();

            serviceDiscovery.start();
            LOGGER.info("Service instance created successfully!");
        } catch (Exception e){
            LOGGER.error("Service instance creation failed! ", e);
        }
    }
}
