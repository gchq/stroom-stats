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

package stroom.stats.properties;

import com.google.common.base.Preconditions;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import stroom.stats.service.config.Config;
import stroom.stats.service.config.ZookeeperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;

public class CuratorFrameworkProvider implements Provider<CuratorFramework> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorFrameworkProvider.class);

    private final ZookeeperConfig zookeeperConfig;

    @Inject
    public CuratorFrameworkProvider(final Config config) {
        this.zookeeperConfig = config.getZookeeperConfig();
        Preconditions.checkNotNull(zookeeperConfig.getPropertyServicePath());
        Preconditions.checkNotNull(zookeeperConfig.getQuorum());
        Preconditions.checkNotNull(zookeeperConfig.getRootPath());
    }

    @Override
    public CuratorFramework get() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        String zookeeperQuorum = zookeeperConfig.getQuorum();
        String zookeeperRootPath = zookeeperConfig.getRootPath();
        String connectionString = zookeeperQuorum + zookeeperRootPath;

        LOGGER.info("Initiating Curator connection to Zookeeper using: ", connectionString);
        //use chroot so all subsequent paths are below /stroom-stats to avoid conflicts with hbase/zookeeper/kafka etc.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();

        try {
            //ensure the chrooted root path exists (i.e. /stroom-stats
            Stat stat = client.checkExists().forPath("/");
            if (stat == null) {
                LOGGER.info("Creating chroot-ed root node inside " + zookeeperRootPath);
                client.create().forPath("/");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error connecting to zookeeper using connection String: " + connectionString, e);
        }
        return client;
    }
}
