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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.data.Stat;
import stroom.stats.service.config.Config;
import stroom.stats.service.config.ZookeeperConfig;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//TODO probably needs moving out into its own module
public class StroomPropertyServiceImpl implements StroomPropertyService {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StroomPropertyServiceImpl.class);

    public static final String PROPERTY_DEFAULTS_FILE_NAME = "default-stroom-stats.properties";


    private final ZookeeperConfig zookeeperConfig;
    private final CuratorFramework curatorFramework;
    private final String propertyServicePath;
    private final TreeCache treeCache;

    private final Semaphore initialisedSemaphore = new Semaphore(0);

    @Inject
    public StroomPropertyServiceImpl(final Config config, @StatsCuratorFramework final CuratorFramework curatorFramework) {
        this.zookeeperConfig = config.getZookeeperConfig();
        this.curatorFramework = curatorFramework;
        propertyServicePath = zookeeperConfig.getPropertyServicePath();
        int initTimeout = zookeeperConfig.getPropertyServiceTreeCacheTimeoutMs();
        ensurePropertyServicePathExists();

        initialisePropertyKeys(config.getDefaultProperties());

        treeCache = TreeCache.newBuilder(curatorFramework, propertyServicePath)
                .setCacheData(true)
                .setMaxDepth(3)
                .build();

        Instant startTime = Instant.now();
        try {
            treeCache.start();
        } catch (Exception e) {
            throw new RuntimeException("Unable to start tree cache", e);
        }
        treeCache.getListenable().addListener(this::treeCacheChangeHandler);
        try {
            //block until the treeCache is ready
            LOGGER.info("Waiting for curator tree cache to be initialised with timeout {}ms", initTimeout);
            initialisedSemaphore.tryAcquire(initTimeout, TimeUnit.MILLISECONDS);
            LOGGER.info(() -> String.format("Initialised treeCache in %sms", ChronoUnit.MILLIS.between(startTime, Instant.now())));
        } catch (InterruptedException e) {
            //reset the interrupt flag
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted while waiting for the treeCache to be initialised");
        }
    }

    private void initialisePropertyKeys(Map<String, String> properties) {
        LOGGER.info("Ensuring property keys exist");
        Properties defaultProps = new Properties();
        defaultProps.putAll(properties);
        defaultProps.stringPropertyNames().stream()
                .sorted()
                .forEach(name -> defaultIfNotExists(name, defaultProps.getProperty(name)));
    }


    /**
     * If the property with the passed name does not exist it will be created with the passed
     * default value, else no change will be made
     *
     * @param name
     * @param defaultValue
     */
    private void defaultIfNotExists(final String name, final String defaultValue) {
        String fullPath = buildPath(name);
        Preconditions.checkArgument(!name.contains(" "), "Property names cannot contain spaces");
        try {
            Stat stat = curatorFramework.checkExists().forPath(fullPath);
            if (stat == null) {
                LOGGER.info("Creating property {} with value {}", name, defaultValue);
                curatorFramework.create().forPath(fullPath, Bytes.toBytes(defaultValue));
            } else {
                LOGGER.debug(() -> String.format("Property %s already exists so leaving it as is", name));
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to set property %s with value %s", name, defaultValue), e);
        }
    }

    private void ensurePropertyServicePathExists() {
        try {
            Stat propertyServiceNode = curatorFramework.checkExists().forPath(propertyServicePath);
            if (propertyServiceNode == null) {
                curatorFramework.create().forPath(propertyServicePath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error ensuring the existence of path: " + propertyServicePath);
        }
    }

    private void treeCacheChangeHandler(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) {
        switch (treeCacheEvent.getType()) {
            case INITIALIZED: {
                //cache is ready so release a permit allowing the application to continue initialising
                initialisedSemaphore.release();
                break;
            }
            case NODE_ADDED: {
                LOGGER.info("Property added: " + childDataToString(treeCacheEvent.getData()));
                break;
            }
            case NODE_REMOVED: {
                LOGGER.info("Property removed: " + childDataToString(treeCacheEvent.getData()));
                break;
            }
            case NODE_UPDATED: {
                LOGGER.info("Property updated: " + childDataToString(treeCacheEvent.getData()));
                break;
            }
            case CONNECTION_LOST: {
                LOGGER.debug("Connection to Zookeeper lost");
                break;
            }
            case CONNECTION_RECONNECTED: {
                LOGGER.debug("Connection to Zookeeper re-established");
                break;
            }
            case CONNECTION_SUSPENDED: {
                LOGGER.debug("Connection to Zookeeper suspended");
                break;
            }
        }
    }

    @Override
    public Optional<String> getProperty(final String name) {
        //TODO this currently assumes all props are cluster wide ones.
        //If we want node specific props in there then we need some form of
        //grouping by cluster node (using some suitable node ID, e.g. FQDN)

        String fullPath = buildPath(name);
        Optional<String> optValue = Optional.ofNullable(treeCache.getCurrentData(fullPath))
                .flatMap(childData -> Optional.ofNullable(childData.getData()))
                .map(bVal -> Bytes.toString(bVal));
        return optValue;
    }

    @Override
    public void setProperty(final String name, final String value) {
        String fullPath = buildPath(name);
        Preconditions.checkArgument(!name.contains(" "), "Property names cannot contain spaces");
        try {
            Stat stat = curatorFramework.checkExists().forPath(fullPath);
            if (stat == null) {
                curatorFramework.create().forPath(fullPath, Bytes.toBytes(value));
            } else {
                curatorFramework.setData().forPath(fullPath, Bytes.toBytes(value));
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to set property %s with value %s", name, value), e);
        }
    }

    @Override
    public List<String> getAllPropertyKeys() {
        Map<String, ChildData> propertyMap = getPropertyMap(propertyServicePath);
        return propertyMap.keySet().stream().collect(Collectors.toList());
    }

    private Map<String, ChildData> getPropertyMap(String propertyServicePath) {
        Map<String, ChildData> propertyMap = treeCache.getCurrentChildren(propertyServicePath);
        if (propertyMap == null) {
            String error = "We can't get properties from ZooKeeper! " +
                    "Is the propertyServicePath configured correctly? " +
                    "Is ZooKeeper available? " +
                    "Has ZooKeeper been pre-loaded with the necessary properties?" +
                    "Has the treeCache been initialised?";
            LOGGER.error(error);
            throw new RuntimeException(error);
        } else {
            return propertyMap;
        }
    }


    private String buildPath(final String propertyName) {
        return propertyServicePath + "/" + propertyName;
    }

    private String childDataToString(final ChildData childData) {
        return new StringBuilder()
                .append(childData.getPath())
                .append(" - ")
                .append(Bytes.toString(childData.getData()))
                .toString();
    }
}
