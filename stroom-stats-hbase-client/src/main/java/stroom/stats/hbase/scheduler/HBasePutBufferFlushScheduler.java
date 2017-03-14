

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

package stroom.stats.hbase.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.table.HBaseTableFactory;
import stroom.stats.properties.StroomPropertyService;

import javax.inject.Inject;
import java.util.concurrent.Semaphore;

public class HBasePutBufferFlushScheduler {
    private final HBaseTableFactory hBaseTableFactory;
    private final StroomPropertyService propertyService;

    // semaphore to prevent multiple cron scheduled jobs from running at once
    private final Semaphore cronSemaphore = new Semaphore(1);

    private static final Logger LOGGER = LoggerFactory.getLogger(HBasePutBufferFlushScheduler.class);

    @Inject
    public HBasePutBufferFlushScheduler(final HBaseTableFactory hBaseTableFactory,
                                        final StroomPropertyService propertyService) {
        this.hBaseTableFactory = hBaseTableFactory;
        this.propertyService = propertyService;
    }

    //TODO need alternative implementation of this scheduling
    //and question whether this complexity is needed at all, instead let HBase's client code handle it
//    @StroomSimpleCronSchedule(cron = "0,10,20,30,40,50 * *")
//    @JobTrackedSchedule(jobName = "HBase Stats Put Buffer Flush", description = "Job to flush the contents of the HBase put buffer")
    public void execute() {
        try {
            // drop out if another cron scheduled job is running
            // this protection is built in to the cron scheduler but this is
            // just a double check
//            if (cronSemaphore.tryAcquire()) {
//                LOGGER.trace("Cron job firing");
//
//                for (final EventStoreTimeIntervalEnum timeInterval : EventStoreTimeIntervalEnum.values()) {
//                    hBaseTableFactory.getEventStoreTable(timeInterval).flushPutBuffer();
//                }
//            }
        } finally {
            cronSemaphore.release();
        }
    }
}
