package stroom.stats.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class HBaseSmokeTestIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSmokeTestIT.class);

    private static final String PROP_KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String PROP_KEY_HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String PROP_KEY_HBASE_ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";
    private static final String PROP_KEY_HBASE_RPC_TIMEOUT_MS = "hbase.rpc.timeout";
    private static final String NAME = "testing";

    @Test
    public void test() throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        configuration.set(PROP_KEY_HBASE_ZOOKEEPER_QUORUM, "localhost");
        configuration.set(PROP_KEY_HBASE_ZOOKEEPER_CLIENT_PORT, "2181");
        configuration.set(PROP_KEY_HBASE_ZOOKEEPER_ZNODE_PARENT, "/hbase");
        configuration.set(PROP_KEY_HBASE_RPC_TIMEOUT_MS, "10000");

        HBaseAdmin.available(configuration);

        Connection sharedClusterConnection = ConnectionFactory.createConnection(configuration);

        Admin admin = sharedClusterConnection.getAdmin();

        TableName tableName = TableName.valueOf(NAME + (new Random()).nextInt(999999));

        final ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("cf123"))
                .setMaxVersions(1)
                .build();

        final TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(columnFamilyDescriptor)
                .build();

        LOGGER.info("Creating table {}", tableName.getNameAsString());

        admin.createTable(tableDescriptor);

        boolean isTableAvailable = admin.isTableAvailable(tableName);

        Assertions.assertThat(isTableAvailable)
                .isTrue();

        LOGGER.info("Deleting table {}", tableName.getNameAsString());
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
}
