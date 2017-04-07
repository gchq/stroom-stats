

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

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

public class TestSimpleRowKeyBuilder {
    private final UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSimpleRowKeyBuilder.class);


    private long buildTimestampFromRowKeyAndColQual(final EventStoreTimeIntervalEnum interval,
            final CellQualifier cellQualifier) {
        final long keyTimestamp = Bytes.toInt(cellQualifier.getRowKey().getPartialTimestamp())
                * interval.rowKeyInterval();
        final long fullTimestamp = keyTimestamp +
                (cellQualifier.getColumnQualifier().getValue() * interval.columnInterval());
        return fullTimestamp;
    }
}
