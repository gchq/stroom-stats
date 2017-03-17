

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

package stroom.stats.hbase.store.task;

import stroom.stats.hbase.structure.CountRowData;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.task.api.ServerTask;
import stroom.stats.task.api.VoidResult;

import java.util.List;

public class HBaseBatchPutTask extends ServerTask<VoidResult> {
    private final transient EventStoreTimeIntervalEnum timeInterval;
    private final transient List<CountRowData> putsBatch;

    public HBaseBatchPutTask(final EventStoreTimeIntervalEnum timeInterval, final List<CountRowData> putsBatch) {
        this.timeInterval = timeInterval;
        this.putsBatch = putsBatch;
    }

    public List<CountRowData> getBatch() {
        return putsBatch;
    }

    public EventStoreTimeIntervalEnum getTimeInterval() {
        return timeInterval;
    }
}
