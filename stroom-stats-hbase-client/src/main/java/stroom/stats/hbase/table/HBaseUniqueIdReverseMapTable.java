

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

package stroom.stats.hbase.table;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.exception.HBaseException;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private HBaseUniqueIdReverseMapTable(final HBaseConnection hBaseConnection) {
        super(hBaseConnection);
        init();
    }

    public static HBaseUniqueIdReverseMapTable getInstance(final HBaseConnection hBaseConnection) {
        return new HBaseUniqueIdReverseMapTable(hBaseConnection);
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
        final HColumnDescriptor colDesc = new HColumnDescriptor(NAME_FAMILY);
        colDesc.setMaxVersions(1);
        desc.addFamily(colDesc);
        return desc;
    }

    @Override
    public boolean checkAndPutName(final byte[] newId, final byte[] name) {
        if (LOGGER.isTraceEnabled()) {
            final String rowKeyStr = ByteArrayUtils.byteArrayToHex(newId);

            final String valueStr = Bytes.toString(name);

            LOGGER.trace("checkAndPutName - Key: [" + rowKeyStr + "], Value: [" + valueStr + "]");
        }

        final Put put = new Put(newId);
        put.addColumn(NAME_FAMILY, NAME_COL_QUALIFIER, name);

        boolean result;

        // pass null as the expected value to ensure we only put if it didn't
        // exist before
        result = doCheckAndPut(newId, NAME_FAMILY, NAME_COL_QUALIFIER, null, put);

        return result;
    }

    @Override
    public boolean checkAndDeleteName(final byte[] newId, final byte[] name) {
        if (LOGGER.isTraceEnabled()) {
            final String rowKeyStr = ByteArrayUtils.byteArrayToHex(newId);

            final String valueStr = Bytes.toString(name);

            LOGGER.trace("checkAndDeleteName - Key: [" + rowKeyStr + "], Value: [" + valueStr + "]");
        }

        final Delete delete = new Delete(newId);
        delete.addColumn(NAME_FAMILY, NAME_COL_QUALIFIER);

        boolean result;

        // only delete the naem if the current value matches what we think it is
        result = doCheckAndDelete(newId, NAME_FAMILY, NAME_COL_QUALIFIER, name, delete);

        return result;
    }

    @Override
    public Optional<byte[]> getName(final byte[] idKey) {
        final Get get = new Get(idKey);
        get.addColumn(NAME_FAMILY, NAME_COL_QUALIFIER);
        final Result result = doGet(get);
        return Optional.ofNullable(result.getValue(NAME_FAMILY, NAME_COL_QUALIFIER));
    }

    @Override
    public Optional<String> getNameAsString(final byte[] idKey) {
        final Optional<byte[]> nameBytes = getName(idKey);

        final Optional<String> name = nameBytes.flatMap(bytes -> Optional.of(Bytes.toString(bytes)));

        return name;
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
