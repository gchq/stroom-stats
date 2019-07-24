

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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.exception.HBaseException;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;

import javax.inject.Inject;
import java.util.Optional;

/**
 * DAO for the HBase UID table
 *
 * Table holds a one way mapping between UIDs and names. Structure looks like
 * this. Only one column family is used
 *
 * @formatter:off
 *
 * 				RowKey n:m n:n
 *                -------------------------------------------------------- \x00
 *                123L \x00\x00\x00\x01 someNameValue \x00\x00\x00\x02
 *                anotherNameValue
 *
 *                The first row with single byte value /x00 is the id generator
 *                row. Its 'v' (for value)column is a long that is incremented
 *                whenever a new ID value is needed. The long id is then
 *                converted into a fixed x byte UID for subsequent storage in
 *                two rows of the table, one for each side of a mapping.
 *
 *
 * @formatter:on
 */
public class HBaseUniqueIdReverseMapTable extends HBaseTable implements UniqueIdReverseMapTable {
    private static final String DISPLAY_NAME = "UniqueIdReverseMap";
    public static final TableName TABLE_NAME = TableName.valueOf(Bytes.toBytes("uidr"));
    public static final byte[] NAME_FAMILY = Bytes.toBytes("n");
    /** Row key of the special row used to track the max ID already assigned. */
    public static final byte[] MAXID_ROW = { 0 };
    // Must be >=1 to avoid a conflict with UniqueIdConstants.NOT_FOUND_UID
    public static final long MAXID_INITIAL_VALUE = 1L;
    public static final byte[] MAXID_COL_QUALIFIER = Bytes.toBytes("m");
    public static final byte[] NAME_COL_QUALIFIER = Bytes.toBytes("n");

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUniqueIdReverseMapTable.class);

    @Inject
    public HBaseUniqueIdReverseMapTable(final HBaseConnection hBaseConnection) {
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
    public TableDescriptor getDesc() {
        final ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                .newBuilder(NAME_FAMILY)
                .setMaxVersions(1)
                .build();

        final TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(getName())
                .setColumnFamily(columnFamilyDescriptor)
                .build();

        return tableDescriptor;
    }

    @Override
    public boolean putNameIfNotExists(final byte[] bNewUid, final byte[] name) {
        if (LOGGER.isTraceEnabled()) {
            final String rowKeyStr = ByteArrayUtils.byteArrayToHex(bNewUid);

            final String valueStr = Bytes.toString(name);

            LOGGER.trace("checkAndPutName - Key: [" + rowKeyStr + "], Value: [" + valueStr + "]");
        }

        final Put put = new Put(bNewUid);
        put.addColumn(NAME_FAMILY, NAME_COL_QUALIFIER, name);

        boolean result;

        // pass null as the expected value to ensure we only put if it didn't
        // exist before
        result = doPutIfNotExists(bNewUid, NAME_FAMILY, NAME_COL_QUALIFIER, put);

        return result;
    }

    @Override
    public boolean checkAndDeleteName(final byte[] bNewUid, final byte[] name) {
        if (LOGGER.isTraceEnabled()) {
            final String rowKeyStr = ByteArrayUtils.byteArrayToHex(bNewUid);

            final String valueStr = Bytes.toString(name);

            LOGGER.trace("checkAndDeleteName - Key: [" + rowKeyStr + "], Value: [" + valueStr + "]");
        }

        final Delete delete = new Delete(bNewUid);
        delete.addColumn(NAME_FAMILY, NAME_COL_QUALIFIER);

        boolean result;

        // only delete the naem if the current value matches what we think it is
        result = doCheckAndDelete(bNewUid, NAME_FAMILY, NAME_COL_QUALIFIER, name, delete);

        return result;
    }

    public Optional<byte[]> getName(final UID uid) {
        final Get get = new Get(uid.getUidBytes());
        get.addColumn(NAME_FAMILY, NAME_COL_QUALIFIER);
        final Result result = doGet(get);
        return Optional.ofNullable(result.getValue(NAME_FAMILY, NAME_COL_QUALIFIER));
    }

    @Override
    public Optional<String> getNameAsString(final UID uid) {
        return getName(uid)
                .map(Bytes::toString);
    }

    @Override
    public long nextId() {
        final Table tableInterface = getTable();
        try {
            final long id = incrementColumnValue(tableInterface, MAXID_ROW, NAME_FAMILY, MAXID_COL_QUALIFIER, 1);

            return id;
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        } finally {
            closeTable(tableInterface);
        }
    }

    @Override
    void tableSpecificCreationProcessing() {
        LOGGER.info("Creating max ID row in Unique ID table");

        final Put put = new Put(MAXID_ROW);
        put.addColumn(NAME_FAMILY, MAXID_COL_QUALIFIER, Bytes.toBytes(MAXID_INITIAL_VALUE));

        doPut(put);
    }
}
