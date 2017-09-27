

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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticTag;
import stroom.stats.common.RollUpBitMaskUtil;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ColumnQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.RowKeyTagValue;
import stroom.stats.hbase.structure.TimeAgnosticRowKey;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.util.DateUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleRowKeyBuilder implements RowKeyBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleRowKeyBuilder.class);

    private final UniqueIdCache uniqueIdCache;
    private final EventStoreTimeIntervalEnum timeInterval;

    /**
     * Constructor.
     *
     * @param uniqueIdCache
     *            The cache to use for UID->String or String->UID lookups
     * @param timeInterval
     *            The time interval to use when building or dealing with row
     *            keys. This time interval controls the granularity of the rows
     *            and column qualifiers.
     */
    public SimpleRowKeyBuilder(final UniqueIdCache uniqueIdCache, final EventStoreTimeIntervalEnum timeInterval) {
        LOGGER.trace("Initialising SimpleRowKeyBuilder");
        this.uniqueIdCache = uniqueIdCache;
        this.timeInterval = timeInterval;
    }

    @Override
    public CellQualifier buildCellQualifier(final StatEventKey statEventKey) {
        Preconditions.checkNotNull(statEventKey);

        long timeMs = statEventKey.getTimeMs();

        final ColumnQualifier columnQualifier = buildColumnQualifier(timeMs);

        //timeMs is already rounded to the column interval
        return new CellQualifier(buildRowKey(statEventKey), columnQualifier, timeMs);
    }

    @Override
    public RowKey buildRowKey(StatEventKey statEventKey) {
        Preconditions.checkNotNull(statEventKey);

        long timeMs = statEventKey.getTimeMs();

        final byte[] partialTimestamp = buildPartialTimestamp(timeMs);
        final byte[] rollupBitMask = statEventKey.getRollupMask().asBytes();
        List<RowKeyTagValue> rowKeyTagValues = statEventKey.getTagValues().stream()
                .map(tagValue -> new RowKeyTagValue(tagValue.getTag(), tagValue.getValue()))
                .collect(Collectors.toList());

        TimeAgnosticRowKey timeAgnosticRowKey = new TimeAgnosticRowKey(
                statEventKey.getStatUuid(),
                rollupBitMask,
                rowKeyTagValues);

        //timeMs is already rounded to the column interval
        return new RowKey(timeAgnosticRowKey, partialTimestamp);
    }


    @Override
    public CellQualifier buildCellQualifier(final RowKey rowKey, final ColumnQualifier columnQualifier) {
        return buildCellQualifier(rowKey, columnQualifier, timeInterval);
    }

    private static CellQualifier buildCellQualifier(
            final RowKey rowKey,
            final ColumnQualifier columnQualifier,
            final EventStoreTimeIntervalEnum timeInterval) {

        final long columnIntervalNo = Bytes.toInt(columnQualifier.getBackingArray(), columnQualifier.getOffset(), ColumnQualifier.length());
        final long columnTimeComponentMillis = columnIntervalNo * timeInterval.columnInterval();
        final long fullTimestamp = getPartialTimestamp(rowKey, timeInterval) + columnTimeComponentMillis;

        return new CellQualifier(rowKey, columnQualifier, fullTimestamp);
    }

    /**
     * Converts a {@link CellQualifier} from one time interval to another so it
     * can be used in a different event store.
     *
     * @param currCellQualifier
     *            The existing cell qualifier
     * @param newTimeInterval
     *            The new time interval to use
     * @return A new instance of a {@link CellQualifier}
     */
    public static CellQualifier convertCellQualifier(final CellQualifier currCellQualifier,
            final EventStoreTimeIntervalEnum newTimeInterval) {
        final byte[] newPartialTimeStamp = buildPartialTimestamp(currCellQualifier.getFullTimestamp(), newTimeInterval);
        final ColumnQualifier newColumnQualifier = buildColumnQualifier(currCellQualifier.getFullTimestamp(), newTimeInterval);

        final RowKey currRowKey = currCellQualifier.getRowKey();

        final RowKey newRowKey = new RowKey(currRowKey.getTypeId(), currRowKey.getRollUpBitMask(), newPartialTimeStamp,
                currRowKey.getTagValuePairs());

        return new CellQualifier(newRowKey, newColumnQualifier,
                newTimeInterval.truncateTimeToColumnInterval(currCellQualifier.getFullTimestamp()));
    }

    public static CellQualifier convertCellQualifier(final RowKey rowKey, final ColumnQualifier columnQualifier,
            final EventStoreTimeIntervalEnum oldTimeInterval, final EventStoreTimeIntervalEnum newTimeInterval) {
        final CellQualifier currCellQualifier = buildCellQualifier(rowKey, columnQualifier, oldTimeInterval);

        return convertCellQualifier(currCellQualifier, newTimeInterval);
    }

    @Override
    public RowKey buildStartKey(final String eventName, final RollUpBitMask rollUpBitMask, final long rangeStartTime) {
        // Get uid for the event name.
        final UID nameUid = uniqueIdCache.getUniqueIdOrDefault(eventName);

        final byte[] partialTimestamp = buildPartialTimestamp(rangeStartTime);

        //tags/values will be handled by the row key filter, hence the empty list
        return new RowKey(nameUid, rollUpBitMask.asBytes(), partialTimestamp, Collections.<RowKeyTagValue> emptyList());
    }

    @Override
    public RowKey buildEndKey(final String eventName, final RollUpBitMask rollUpBitMask, final long rangeEndTime) {
        // Get uid for the event name.
        final UID nameUid = uniqueIdCache.getUniqueIdOrDefault(eventName);

        // add one interval on to give us the next row key to the one we want
        final byte[] partialTimestamp = buildPartialTimestamp(rangeEndTime + timeInterval.rowKeyInterval());

        //tags/values will be handled by the row key filter, hence the empty list
        return new RowKey(nameUid, rollUpBitMask.asBytes(), partialTimestamp, Collections.<RowKeyTagValue> emptyList());
    }

    @Override
    public String getNamePart(final RowKey rowKey) {
        return uniqueIdCache.getName(rowKey.getTypeId());
    }

    @Override
    public long getPartialTimestamp(final RowKey rowKey) {
        return getPartialTimestamp(rowKey, this.timeInterval);
    }

    public static long getPartialTimestamp(final RowKey rowKey, final EventStoreTimeIntervalEnum timeInterval) {
        final long IntervalNo = Bytes.toInt(rowKey.getPartialTimestamp());
        final long intervalSizeMillis = timeInterval.rowKeyInterval();
        final long partialTimestampMillis = IntervalNo * intervalSizeMillis;
        return partialTimestampMillis;
    }

    @Override
    public Map<String, String> getTagValuePairsAsMap(final RowKey rowKey) {
        return TagValueConverter.getTagValuePairsAsMap(rowKey.getTagValuePairs(), uniqueIdCache);
    }

    // TODO Need to decide if we should return this as a list or a map
    @Override
    public List<StatisticTag> getTagValuePairsAsList(final RowKey rowKey) {
        return TagValueConverter.getTagValuePairsAsList(rowKey.getTagValuePairs(), uniqueIdCache);
    }

    @Override
    public String toPlainTextString(final RowKey rowKey) {
        final StringBuilder sb = new StringBuilder();
        boxString(sb, getNamePart(rowKey));
        boxString(sb, RollUpBitMask.fromBytes(rowKey.getRollUpBitMask()).asHexString());

        final long partialTimestamp = getPartialTimestamp(rowKey);
        final String rowKeyTimeRange = DateUtil.createNormalDateTimeString(partialTimestamp) + " - "
                + DateUtil.createNormalDateTimeString(partialTimestamp + timeInterval.rowKeyInterval() - 1L);
        boxString(sb, rowKeyTimeRange);
        for (final StatisticTag statisticTag : getTagValuePairsAsList(rowKey)) {
            sb.append("[");
            boxString(sb, statisticTag.getTag());
            boxString(sb, statisticTag.getValue());
            sb.append("]");
        }
        return sb.toString();
    }

    private byte[] buildPartialTimestamp(final long time) {
        return buildPartialTimestamp(time, timeInterval);
    }

    private byte[] buildRollUpBitMask(final List<StatisticTag> tagList) {
        final byte[] rollUpBitMask = RollUpBitMaskUtil.fromSortedTagList(tagList).asBytes();
        return rollUpBitMask;
    }

    public static byte[] buildPartialTimestamp(final long time, final EventStoreTimeIntervalEnum timeInterval) {
        // event time is in millis, the desired time interval is in millis so
        // divide one
        // by the other to get the number of intervals since the epoch
        // and use this as the row key time component
        final int timeIntervalNo = (int) (time / timeInterval.rowKeyInterval());

        final byte[] partialTimestamp = Bytes.toBytes(timeIntervalNo);
        // final byte[] partialTimestamp = UnsignedBytes.toBytes(4,
        // timeIntervalNo);

        return partialTimestamp;
    }

    private ColumnQualifier buildColumnQualifier(final long time) {
        return buildColumnQualifier(time, timeInterval);
    }

    private static ColumnQualifier buildColumnQualifier(final long time, final EventStoreTimeIntervalEnum timeInterval) {
        // the row key has a rounded time. We must then work out the column
        // qualifier time
        // This uses a finer grained interval, e.g. key is hourly and columns
        // are rounded to the second.
        // So we work out which second in the hour the event falls into
        final long remainderTimeMillis = (time % timeInterval.rowKeyInterval());
        final byte[] bColumnQualifier = Bytes.toBytes((int) (remainderTimeMillis / timeInterval.columnInterval()));
        return ColumnQualifier.from(bColumnQualifier);
    }

    private void boxString(final StringBuilder sb, final String text) {
        sb.append("[");
        sb.append(text);
        sb.append("]");
    }
}
