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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import stroom.stats.service.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.service.config.ZookeeperConfig;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

public class StatsCuratorFrameworkProvider implements Provider<CuratorFramework> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsCuratorFrameworkProvider.class);
    private ZookeeperConfig zookeeperConfig;

    @Inject
    public StatsCuratorFrameworkProvider(final Config config) {
        this.zookeeperConfig = config.getZookeeperConfig();
    }

    @Override
    public CuratorFramework get() {
        String quorum = zookeeperConfig.getQuorum();
        String statsPath = zookeeperConfig.getStatsPath();
        String connectionString = quorum + statsPath;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        LOGGER.info("Initiating Curator connection to Zookeeper using: ", connectionString);
        // Use chroot so all subsequent paths are below /stroom-stats to avoid conflicts with hbase/zookeeper/kafka etc.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        client.start();

        try {
            // Ensure the chrooted root path exists (i.e. /stroom-stats)
            Stat stat = client.checkExists().forPath("/");
            if (stat == null) {
                LOGGER.info("Creating chroot-ed root node inside " + statsPath);
                client.create().forPath("/");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error connecting to zookeeper using connection String: " + connectionString, e);
        }
        return client;
    }
}
