

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

package stroom.stats.hbase.store.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.structure.CountRowData;
import stroom.stats.hbase.table.TableFactory;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.task.api.AbstractTaskHandler;
import stroom.stats.task.api.VoidResult;

import javax.inject.Inject;
import java.util.List;

//@TaskHandlerBean(task = HBaseBatchPutTask.class)
//@Scope(value = StroomScope.TASK)
public class HBaseBatchPutTaskHandler extends AbstractTaskHandler<HBaseBatchPutTask, VoidResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBatchPutTaskHandler.class);

    private int total;

//    private TaskMonitor taskMonitor;

    private TableFactory tableFactory;

    @Inject
    public HBaseBatchPutTaskHandler(final TableFactory tableFactory) {
        this.tableFactory = tableFactory;
    }

    @Override
    public VoidResult exec(final HBaseBatchPutTask task) {
        putBatch(task.getTimeInterval(), task.getBatch());
        return new VoidResult();
    }

    private void putBatch(final EventStoreTimeIntervalEnum timeInterval, final List<CountRowData> putsBatch) {
        if (putsBatch != null && putsBatch.size() > 0) {
            total = putsBatch.size();

            // LOGGER.debug("Putting multiple counts to HBase (count=%s) for
            // store: %s", total, timeInterval);
//            taskMonitor.info("Putting multiple counts to HBase (count=%s)", total);

            // this is a HBase specific task so use the concrete class rather
            // than the interface
//            final HBaseEventStoreTable eventStoreTable = (HBaseEventStoreTable) tableFactory
//                    .getEventStoreTable(timeInterval);
//
//            try {
////                eventStoreTable.addMultipleCounts(putsBatch);
//
//            } catch (final Exception ex) {
//                LOGGER.error("putBatch() - Failed", ex);
//                throw ex;
//            }
        }
    }
}
