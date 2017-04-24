

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

import stroom.stats.api.StatisticTag;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ColumnQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.streams.StatKey;

import java.util.List;
import java.util.Map;

/**
 * Decorator for a {@link SimpleRowKeyBuilder} which adds caching of
 * event->timeAgnosticRowKey. Speeds up the creation of row keys if there are
 * lots of events with the same time agnostic elements.
 */
public class CachedRowKeyBuilder implements RowKeyBuilder {
    private final SimpleRowKeyBuilder rowKeyBuilder;

    private CachedRowKeyBuilder(final SimpleRowKeyBuilder rowKeyBuilder) {
        this.rowKeyBuilder = rowKeyBuilder;
    }

    public static RowKeyBuilder wrap(final SimpleRowKeyBuilder rowKeyBuilder) {
        return new CachedRowKeyBuilder(rowKeyBuilder);
    }

    /**
     * Looks up the time agnostic parts of the event in a row key cache to see
     * if a time agnostic row key has already been built. If it is not found the
     * cache will build the key on the fly and cache it.
     */


    @Override
    public CellQualifier buildCellQualifier(final RowKey rowKey, final ColumnQualifier columnQualifier) {
        return rowKeyBuilder.buildCellQualifier(rowKey, columnQualifier);
    }

    @Override
    public CellQualifier buildCellQualifier(StatKey statKey) {
        return rowKeyBuilder.buildCellQualifier(statKey);
    }

    @Override
    public RowKey buildRowKey(StatKey statKey) {
        return rowKeyBuilder.buildRowKey(statKey);
    }

    @Override
    public RowKey buildStartKey(final String eventName, final RollUpBitMask rollUpBitMask, final long rangeStartTime) {
        return rowKeyBuilder.buildStartKey(eventName, rollUpBitMask, rangeStartTime);
    }

    @Override
    public RowKey buildEndKey(final String eventName, final RollUpBitMask rollUpBitMask, final long rangeEndTime) {
        return rowKeyBuilder.buildEndKey(eventName, rollUpBitMask, rangeEndTime);
    }

    @Override
    public String getNamePart(final RowKey rowKey) {
        return rowKeyBuilder.getNamePart(rowKey);
    }

    @Override
    public long getPartialTimestamp(final RowKey rowKey) {
        return rowKeyBuilder.getPartialTimestamp(rowKey);
    }

    @Override
    public Map<String, String> getTagValuePairsAsMap(final RowKey rowKey) {
        return rowKeyBuilder.getTagValuePairsAsMap(rowKey);
    }

    @Override
    public List<StatisticTag> getTagValuePairsAsList(final RowKey rowKey) {
        return rowKeyBuilder.getTagValuePairsAsList(rowKey);
    }

    @Override
    public String toPlainTextString(final RowKey rowKey) {
        return rowKeyBuilder.toPlainTextString(rowKey);
    }

}
