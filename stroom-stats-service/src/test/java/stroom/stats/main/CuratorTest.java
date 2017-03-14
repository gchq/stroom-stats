/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.main;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CuratorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorTest.class);

    public static void main(String[] args) throws Exception {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        //use chroot so all subsequent paths are below /stroom-stats to avoid conflicts with hbase/zookeeper/kafka etc.
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181/stroom-stats", retryPolicy);
        client.start();

        Stat stat = client.checkExists().forPath("/");
        if (stat == null) {
            LOGGER.info("Creating root node");
            client.create().forPath("/");
        }

        Stat testNode = client.checkExists().creatingParentContainersIfNeeded().forPath("/my/test/path");

//        dumpChildren(client, "/");
//        dumpChildren(client, "/propertyService");

        checkTreeCache(client, "/propertyService");


//        client.create().forPath("/testNode", Bytes.toBytes("MyStringVal"));

        byte[] bVal = client.getData().forPath("/testNode");
        LOGGER.info(Bytes.toString(bVal));


        client.close();
    }


    private static void dumpChildren(CuratorFramework client, String path) {
        LOGGER.info("Dumping children for " + path);
        try {
            client.getChildren().forPath(path).forEach(System.out::println);
        } catch (Exception e) {
            LOGGER.error("Error dumping children", e);
        }

    }

    private static void checkTreeCache(CuratorFramework curatorFramework, String path) throws Exception {
        final Semaphore semaphore = new Semaphore(0);
        TreeCache treeCache = TreeCache.newBuilder(curatorFramework, path)
                .setCacheData(true)
                .setMaxDepth(3)
                .build();

        if (treeCache == null) {
            LOGGER.error("treeCache is null");
        }

        treeCache.getListenable().addListener((client, event) -> {
            if (event.getType().equals(TreeCacheEvent.Type.INITIALIZED)) {
                semaphore.release();
            }
        });

        treeCache.start();

        semaphore.tryAcquire(2, TimeUnit.SECONDS);


        Map<String, ChildData> map = treeCache.getCurrentChildren("/propertyService");

       if (map == null) {
           LOGGER.error("map is null");
       }

        map.entrySet().forEach(entry -> {
            LOGGER.info("{} - {}", entry.getKey(), Bytes.toString(entry.getValue().getData()));
        });
    }
}
