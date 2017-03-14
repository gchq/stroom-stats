

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

import stroom.stats.hbase.aggregator.AbstractInMemoryEventStore;
import stroom.stats.task.api.ServerTask;
import stroom.stats.task.api.VoidResult;

public class EventStoreFlushTask extends ServerTask<VoidResult> {
    private final transient AbstractInMemoryEventStore storeToFlush;
    private final transient boolean isForcedFlushToDisk;

    /**
     * @param storeToFlush
     *            The in memory event store to flush
     * @param isForcedFlushToDisk
     *            Used to ensure the flush goes all the way down to persistent
     *            storage. Typically used in a shutdown situation
     */
    public EventStoreFlushTask(final AbstractInMemoryEventStore storeToFlush, final boolean isForcedFlushToDisk) {
        this.storeToFlush = storeToFlush;
        this.isForcedFlushToDisk = isForcedFlushToDisk;
    }

    public EventStoreFlushTask(final AbstractInMemoryEventStore storeToFlush) {
        this.storeToFlush = storeToFlush;
        this.isForcedFlushToDisk = false;
    }

    public AbstractInMemoryEventStore getMap() {
        return storeToFlush;
    }

    /**
     * @return True if this flush needs to go all thr way down to disk rather
     *         than being buffered further on
     */
    public boolean isForcedFlushToDisk() {
        return isForcedFlushToDisk;
    }
}
