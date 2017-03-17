

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

package stroom.stats.hbase.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.table.HBaseEventStoreTableFactory;
import stroom.stats.properties.StroomPropertyService;

import javax.inject.Inject;
import java.util.concurrent.Semaphore;

public class HBasePutBufferFlushScheduler {
    private final HBaseEventStoreTableFactory hBaseTableFactory;
    private final StroomPropertyService propertyService;

    // semaphore to prevent multiple cron scheduled jobs from running at once
    private final Semaphore cronSemaphore = new Semaphore(1);

    private static final Logger LOGGER = LoggerFactory.getLogger(HBasePutBufferFlushScheduler.class);

    @Inject
    public HBasePutBufferFlushScheduler(final HBaseEventStoreTableFactory hBaseTableFactory,
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
