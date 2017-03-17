

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

package stroom.stats.hbase.table;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

/**
 * DAO for the HBase UID table
 *
 * Table holds a one way mapping between names and UIDs. Structure looks like
 * this. Only one column family is used
 *
 * @formatter:off
 *
 * 				RowKey i:i
 *                --------------------------------------------------------
 *                someNameValue \x00\x00\x00\x01 anotherNameValue
 *                \x00\x00\x00\x02
 *
 * @formatter:on
 */
public class HBaseUniqueIdForwardMapTable extends HBaseTable implements UniqueIdForwardMapTable {
    private static final String DISPLAY_NAME = "UniqueIdForwardMap";
    private static final TableName TABLE_NAME = TableName.valueOf(Bytes.toBytes("uidf"));
    private static final byte[] ID_FAMILY = Bytes.toBytes("i");
    private static final byte[] ID_COL_QUALIFIER = Bytes.toBytes("i");

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUniqueIdForwardMapTable.class);

    @Inject
    public HBaseUniqueIdForwardMapTable(final HBaseConnection hBaseConnection) {
        super(hBaseConnection);
        init();
    }

    @Override
    public TableName getName() {
        return TABLE_NAME;
    }

    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }

    @Override
    public HTableDescriptor getDesc() {
        final HTableDescriptor desc = new HTableDescriptor(getName());
        final HColumnDescriptor colDesc = new HColumnDescriptor(ID_FAMILY);
        colDesc.setMaxVersions(1);
        desc.addFamily(colDesc);
        return desc;
    }

    @Override
    public Optional<byte[]> getId(final byte[] nameKey) {
        return getValue(nameKey);
    }

    @Override
    public boolean checkAndPutId(final byte[] nameKey, final byte[] newId) {
        return checkAndPutKeyValue(nameKey, newId);
    }

    private Optional<byte[]> getValue(final byte[] key) {
        final Get get = new Get(key);
        get.addColumn(ID_FAMILY, ID_COL_QUALIFIER);
        final Result result = doGet(get);
        return Optional.ofNullable(result.getValue(ID_FAMILY, ID_COL_QUALIFIER));
    }

    private boolean checkAndPutKeyValue(final byte[] nameKey, final byte[] newId) {
        if (LOGGER.isTraceEnabled()) {
            final String rowKeyStr = Bytes.toString(nameKey);

            final String valueStr = ByteArrayUtils.byteArrayToHex(newId);

            LOGGER.trace("checkAndPutKeyValue - Key: [" + rowKeyStr + "], Value: [" + valueStr + "]");
        }

        final Put put = new Put(nameKey);
        put.addColumn(ID_FAMILY, ID_COL_QUALIFIER, newId);

        boolean result;

        result = doCheckAndPut(nameKey, ID_FAMILY, ID_COL_QUALIFIER, null, put);

        return result;
    }
}
