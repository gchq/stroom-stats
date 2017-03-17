

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.exception.HBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class HBaseTable implements GenericTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTable.class);

    private final HBaseConnection hBaseConnection;

    public HBaseTable(final HBaseConnection hBaseConnection) {
        this.hBaseConnection = hBaseConnection;
    }

    /**
     * Check if the table already exists, create if needed and then open.
     */
    void init() {
        // HBaseAdmin admin = null;
        boolean isTableBeingCreated = false;
        try (Admin admin = hBaseConnection.getConnection().getAdmin()) {
            if (admin.isTableAvailable(getName())) {
                LOGGER.info("Found HBase table '{}'", getDisplayName());

            } else {
                if (getTableConfiguration().isAutoCreateTables()) {
                    LOGGER.info("HBase table '{}' could not be found, so will create it", getDisplayName());
                    create(admin);
                    isTableBeingCreated = true;

                } else {
                    final String message = "Table  '" + getDisplayName() + "' does not exist";
                    LOGGER.error(message);
                    throw new HBaseException(message);
                }
            }

            // table.setAutoFlush(true, true);
            // table.setWriteBufferSize(getWriteBufferSizeBytes());

            if (isTableBeingCreated) {
                tableSpecificCreationProcessing();
            }

        } catch (final Throwable t) {
            throw new HBaseException(t.getMessage(), t);
        }
    }

    private void create(final Admin admin) {
        try {
            LOGGER.info("Creating table '{}'", getDisplayName());
            admin.createTable(getDesc());
        } catch (final Exception e) {
            throw new HBaseException(e.getMessage(), e);
        }
    }

    public static void closeScanner(final ResultScanner scanner) {
        try {
            if (scanner != null) {
                scanner.close();
            }
        } catch (final Exception e) {
            throw new HBaseException(e.getMessage(), e);
        }
    }

    public static void closeTable(final Table table) {
        try {
            if (table != null) {
                table.close();
            }
        } catch (final Exception e) {
            throw new HBaseException(e.getMessage(), e);
        }
    }

    /**
     * Gets a tableInterface, does the passed put on this table and then closes
     * the tableInterface
     *
     * @param put
     *            The HBase put to put
     */
    public void doPut(final Put put) {
        final Table tableInterface = getTable();
        try {
            doPut(tableInterface, put);
        } finally {
            closeTable(tableInterface);
        }
    }

    /**
     * Does the put using the passed tableInterface but leaves it open
     *
     * @param tableInterface
     * @param put
     */
    public static void doPut(final Table tableInterface, final Put put) {
        try {
            tableInterface.put(put);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
    }

    public void doPuts(final List<Put> puts) {
        final Table tableInterface = getTable();
        try {
            doPuts(tableInterface, puts);
        } finally {
            closeTable(tableInterface);
        }
    }

    /**
     * Does the puts using the passed tableInterface but leaves it open
     *
     * @param tableInterface
     * @param puts
     */
    public static void doPuts(final Table tableInterface, final List<Put> puts) {
        try {
            tableInterface.put(puts);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
    }

    boolean doCheckAndPut(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
            final Put put) {
        boolean result;
        final Table tableInterface = getTable();
        try {
            result = doCheckAndPut(tableInterface, row, family, qualifier, value, put);
        } finally {
            closeTable(tableInterface);
        }
        return result;
    }

    public static boolean doCheckAndPut(final Table tableInterface, final byte[] row, final byte[] family,
            final byte[] qualifier, final byte[] value, final Put put) {
        boolean result;
        try {
            result = tableInterface.checkAndPut(row, family, qualifier, value, put);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
        return result;
    }

    boolean doCheckAndDelete(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
            final Delete delete) {
        boolean result;
        final Table tableInterface = getTable();
        try {
            result = doCheckAndDelete(tableInterface, row, family, qualifier, value, delete);
        } finally {
            closeTable(tableInterface);
        }
        return result;
    }

    public static boolean doCheckAndDelete(final Table tableInterface, final byte[] row, final byte[] family,
            final byte[] qualifier, final byte[] value, final Delete delete) {
        boolean result;
        try {
            result = tableInterface.checkAndDelete(row, family, qualifier, value, delete);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * Gets a Table for this table, does the get and closes the Table
     */
    public Result doGet(final Get get) {
        Result result;
        final Table tableInterface = getTable();
        try {
            result = doGet(tableInterface, get);
        } finally {
            closeTable(tableInterface);
        }
        return result;
    }

    /**
     * Does the get on the passed Table and leaves it open
     */
    public static Result doGet(final Table tableInterface, final Get get) {
        Result result;

        try {
            result = tableInterface.get(get);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * Gets a tableInterface, does the passed delete on this table and then
     * closes the tableInterface
     *
     * @param delete
     *            The HBase delete object
     */
    public void doDelete(final Delete delete) {
        final Table tableInterface = getTable();
        try {
            doDelete(delete);
        } finally {
            closeTable(tableInterface);
        }
    }

    /**
     * Does the delete using the passed tableInterface but leaves it open
     *
     * @param tableInterface
     * @param delete
     */
    public static void doDelete(final Table tableInterface, final Delete delete) {
        try {
            tableInterface.delete(delete);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
    }

    /**
     * Wraps a HBase batch call. Gets the Table for this table, calls batch then
     * closes the Table
     */
    void doBatch(final List<? extends Row> actions, final Object[] results) {
        final Table tableInterface = getTable();
        try {
            tableInterface.batch(actions, results);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        } finally {
            closeTable(tableInterface);
        }
    }

    /**
     * Wrapper on the HBase method. Gets a Table for this table, does the
     * increment and closes the table
     *
     * @param row
     * @param family
     * @param qualifier
     * @param amount
     * @return
     */
    public long incrementColumnValue(final byte[] row, final byte[] family, final byte[] qualifier, final long amount) {
        long result;
        final Table tableInterface = getTable();
        try {
            result = incrementColumnValue(tableInterface, row, family, qualifier, amount);
        } catch (final Exception e) {
            throw new HBaseException(e.getMessage(), e);
        } finally {
            closeTable(tableInterface);
        }
        return result;
    }

    /**
     * Does the increment using the tableInterface but leaves it open for
     * further use
     */
    public long incrementColumnValue(final Table tableInterface, final byte[] row, final byte[] family,
            final byte[] qualifier, final long amount) {
        long result;
        try {
            result = tableInterface.incrementColumnValue(row, family, qualifier, amount);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
        return result;
    }

    public BufferedMutator getBufferedMutator(final ExceptionListener exceptionListener) {
        final BufferedMutatorParams params = new BufferedMutatorParams(getName()).listener(exceptionListener);

        BufferedMutator bufferedMutator;
        try {
            bufferedMutator = hBaseConnection.getConnection().getBufferedMutator(params);
        } catch (final Exception e) {
            throw new HBaseException("Unable to create buffered mutator for table " + getDisplayName(), e);
        }

        return bufferedMutator;
    }

    /**
     * Gets a scanner object and handles any exception
     */
    public ResultScanner getScanner(final Scan scan) {
        final Table tableInterface = getTable();
        return getScanner(tableInterface, scan);
    }

    /**
     * Gets a scanner object and handles any exception
     */
    public static ResultScanner getScanner(final Table tableInterface, final Scan scan) {
        try {
            return tableInterface.getScanner(scan);
        } catch (final Exception e) {
            closeTable(tableInterface);
            throw new HBaseException(e.getMessage(), e);
        }
    }

    /**
     * @return An {@link Table} instance for use by a single thread to interact
     *         with HBase. According to HBase docs, this is a cheap call so
     *         should not be pooled or cached, so get it, use it then close it.
     */
    public Table getTable() {
        return hBaseConnection.getTable(getName());
    }

    public abstract TableName getName();

    @Override
    public String getNameAsString() {
        return getName().getNameAsString();
    }

    @Override
    public abstract String getDisplayName();

    public abstract HTableDescriptor getDesc();

    void tableSpecificCreationProcessing() {
        // Do nothing in here as this is designed to be overridden by
        // sub-classes if they need to run any table specific
        // code after creation.
    }

    @Override
    public HBaseConnection getTableConfiguration() {
        return hBaseConnection;
    }

    public Configuration getConfiguration() {
        return hBaseConnection.getConfiguration();
    }
}
