

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

package stroom.stats.hbase;

public interface HBaseStatisticConstants {
    // Memory store properties

    String THREAD_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME = "stroom.stats.hbase.memoryStore.threadSpecific.maxSize";

    String THREAD_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME_PREFIX = "stroom.stats.hbase.memoryStore.threadSpecific.timeoutMillis.";

    String THREAD_SPECIFIC_MEM_STORE_ID_POOL_SIZE_PROPERTY_NAME_PREFIX = "stroom.stats.hbase.memoryStore.threadSpecific.idPoolSize.";

    String NODE_SPECIFIC_MEM_STORE_MAX_SIZE_PROPERTY_NAME = "stroom.stats.hbase.memoryStore.nodeSpecific.maxSize";

    String NODE_SPECIFIC_MEM_STORE_TIMEOUT_MS_PROPERTY_NAME_PREFIX = "stroom.stats.hbase.memoryStore.nodeSpecific.timeoutMillis.";

    String NODE_SPECIFIC_MEM_STORE_FLUSH_TASK_COUNT_LIMIT_PROPERTY_NAME_PREFIX = "stroom.stats.hbase.memoryStore.nodeSpecific.flushTaskCountLimit.";

    // Data store properties

    String DATA_STORE_MAX_CHECK_AND_PUT_RETRIES_PROPERTY_NAME = "stroom.stats.hbase.dataStore.maxCheckAndPutRetries";

    String DATA_STORE_PUT_BUFFER_MAX_SIZE_PROPERTY_NAME = "stroom.stats.hbase.dataStore.putBuffer.maxSize";

    String DATA_STORE_PUT_BUFFER_TAKE_COUNT_PROPERTY_NAME = "stroom.stats.hbase.dataStore.putBuffer.takeCount";

    String DATA_STORE_PURGE_MAX_BATCH_PUT_TASKS = "stroom.stats.hbase.dataStore.putBuffer.maxBatchPutTasks";

    String DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX = "stroom.stats.hbase.dataStore.purge.intervalsToRetain.";

    // HBase stats search properties

    String SEARCH_MAX_INTERVALS_IN_PERIOD_PROPERTY_NAME = "stroom.stats.hbase.search.maxTimeIntervalsInPeriod";

    // HBase state store properties

    String STATE_STORE_PUT_BUFFER_DELAY_PROPERTY_NAME = "stroom.stats.hbase.stateStore.putBuffer.delayMs";

    String STATE_STORE_PUT_BUFFER_MAX_CHANGES_PROPERTY_NAME = "stroom.stats.hbase.stateStore.putBuffer.maxChangesInBuffer";

    // HBase connection/configuration properties. These are properties to hold
    // values for HBase's own configuration
    // properties.
    String HBASE_ZOOKEEPER_QUORUM_PROPERTY_NAME = "stroom.stats.hbase.config.zookeeper.quorum";

    String HBASE_ZOOKEEPER_CLIENT_PORT_PROPERTY_NAME = "stroom.stats.hbase.config.zookeeper.property.clientPort";

    String HBASE_RPC_TIMEOUT_MS_PROPERTY_NAME = "stroom.stats.hbase.config.rpc.timeout";

    String ANALYTIC_OUTPUT_FILTER_BUFFER_SIZE_PROPERTY_NAME = "stroom.stats.analyticOutput.filter.bufferSize";
}
