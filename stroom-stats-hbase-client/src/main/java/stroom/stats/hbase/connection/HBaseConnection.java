

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

package stroom.stats.hbase.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.exception.HBaseException;
import stroom.stats.properties.StroomPropertyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Singleton instance to hold the HBaseConfiguration object which contains the connection to HBase.
 * <p>
 * A dependency on this class will result in a connection being made to Zookeeper
 */
@Singleton
public class HBaseConnection {
    private final Configuration configuration;
    private final boolean autoCreateTables;
    // private final HTablePool pool;

    // This is a connection object that should be shared by all processes that
    // need to talk to HBase
    // It takes the overhead of opening the connection to ZooKeeper and the
    // region server so code only needs to
    // request a new HTableInterface from it using getTable
    private final Connection sharedClusterConnection;

    // HBase property names for configuring HBase
    private static final String HBASE_ZOOKEEPER_QUORUM_PROPERTY_NAME = "hbase.zookeeper.quorum";
    private static final String HBASE_ZOOKEEPER_CLIENT_PORT_PROPERTY_NAME = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_RPC_TIMEOUT_MS_PROPERTY_NAME = "hbase.rpc.timeout";

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnection.class);

    @Inject
    public HBaseConnection(final StroomPropertyService propertyService) {

        final String quorum = propertyService.getPropertyOrThrow(
                HBaseStatisticConstants.HBASE_ZOOKEEPER_QUORUM_PROPERTY_NAME);

        LOGGER.info("Initialising HBaseTableConfiguration to quorum: {}", quorum);

        configuration = HBaseConfiguration.create();

        // if you want the local hbase instance rather than the ref cluster
        // comment these two out
        configuration.set(HBASE_ZOOKEEPER_QUORUM_PROPERTY_NAME, quorum);

        configuration.set(HBASE_ZOOKEEPER_CLIENT_PORT_PROPERTY_NAME,
                propertyService.getPropertyOrThrow(HBaseStatisticConstants.HBASE_ZOOKEEPER_CLIENT_PORT_PROPERTY_NAME));

        configuration.set(HBASE_RPC_TIMEOUT_MS_PROPERTY_NAME,
                propertyService.getPropertyOrThrow(HBaseStatisticConstants.HBASE_RPC_TIMEOUT_MS_PROPERTY_NAME));

        autoCreateTables = true;

        //test the connection to HBase to avoid NPEs bubbling up form inside the HBase code
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);
        } catch (final Exception e) {
            LOGGER.error("Error while testing connection to HBase with zookeeper quorum [" + quorum +
                    "]. HBase may be down or the configuration may be incorrect", e);
            LOGGER.info("Shutting down the system due to lack of an HBase connection");
            System.exit(1);
        }

        try {
            sharedClusterConnection = ConnectionFactory.createConnection(configuration);
        } catch (final IOException e) {
            LOGGER.error("Unable to open a connection to HBase", e);
            throw new HBaseException("Unable to open a connection to HBase", e);
        }

        LOGGER.info("HBaseTableConfiguration initialised");
    }

    /**
     * Alternative constructor for when you already have an hbase connection
     * object you want to use
     */
    public HBaseConnection(final Connection connection) {
        this.sharedClusterConnection = connection;
        this.configuration = connection.getConfiguration();
        autoCreateTables = true;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public boolean isAutoCreateTables() {
        return autoCreateTables;
    }

    public Table getTable(final TableName tableName) {
        // return pool.getTable(tableName.getName());
        try {
            return sharedClusterConnection.getTable(tableName);
        } catch (final IOException e) {
            throw new HBaseException(
                    "Unable get an HTable from the shared connection for table name " + tableName.getNameAsString(), e);
        }
    }

    public Connection getConnection() {
        return sharedClusterConnection;
    }

    //TODO Need alternative implementation of shutdown hook
    public void shutdown() {
        try {
            final Connection connection = getConnection();
            if (connection != null) {
                connection.close();
            }
        } catch (final IOException e) {
            throw new HBaseException("Unable close HBase connection", e);
        }
    }
}
