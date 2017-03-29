

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
import stroom.stats.api.StatisticTag;
import stroom.stats.api.TimeAgnosticStatisticEvent;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.structure.ColumnQualifier;
import stroom.stats.hbase.structure.RowKey;
import stroom.stats.hbase.structure.TimeAgnosticRowKey;
import stroom.stats.streams.StatKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface RowKeyBuilder {
    /**
     * Builds a {@link CellQualifier} object from an event
     *
     * @param rolledUpStatisticEvent
     *            The event to convert into a {@link CellQualifier} object
     * @return The event as a {@link CellQualifier}
     */
    // public abstract CellQualifier buildCellQualifier(StatisticEvent
    // statisticEvent);

    List<CellQualifier> buildCellQualifiers(RolledUpStatisticEvent rolledUpStatisticEvent);

    CellQualifier buildCellQualifier(final StatKey statKey);

    RowKey buildRowKey(final StatKey statKey);

    /**
     * Constructs a {@link CellQualifier} object for a given time. This is
     * intended for use in retrieval of data within a time range.
     *
     * @param eventName
     *            The name of the event which becomes the first 4 bytes of the
     *            row key
     * @param rangeStartTime
     *            The time in millis (from the start of the epoch).
     * @return A Cell object ready for use in HBase get/scan queries
     */
    // public abstract CellQualifier buildCellQualifier(String eventName, final
    // RollUpBitMask rollUpBitMask,
    // long rangeStartTime);

    /**
     * Constructs a {@link CellQualifier} object for a given rowkey object and
     * column qualifier byte array.
     *
     * @param rowKey
     *            The rowKey object as constructed from the HBase row key byte
     *            array
     * @param columnQualifier
     *            The byte array that makes up the column qualifier
     * @return
     */
    CellQualifier buildCellQualifier(RowKey rowKey, ColumnQualifier columnQualifier);

    /**
     * Constructs a RowKey object for the start of a time range. This is
     * intended for use in retrieval of data within a time range.
     *
     * @param eventName
     *            The name of the event which becomes the first 4 bytes of the
     *            row key
     * @param rangeStartTime
     *            The time in millis (from the start of the epoch). Range start
     *            time is inclusive
     * @return A row key object for the start time.
     */
    RowKey buildStartKey(String eventName, final RollUpBitMask rollUpBitMask, long rangeStartTime);

    default byte[] buildStartKeyBytes(final String eventName, final RollUpBitMask rollUpBitMask, final long rangeStartTime) {
       return buildStartKey(eventName, rollUpBitMask, rangeStartTime).asByteArray();
    }

    default byte[] buildStartKeyBytes(final String eventName, final RollUpBitMask rollUpBitMask) {
        //the time portion is irrelevant so just use 0
        RowKey rowKey = buildStartKey(eventName, rollUpBitMask, 0L);
        return rowKey.getTimeAgnosticRowKey().asPartialKey();
    }


    /**
     * Constructs a RowKey object for the end of a time range. This is intended
     * for use in retrieval of data within a time range.
     *
     * @param eventName
     *            The name of the event which becomes the first 4 bytes of the
     *            row key
     * @param rangeEndTime
     *            The time in millis (from the start of the epoch). Range end
     *            time is exclusive
     * @return A row key object for the end time. HBase get/scan operations with
     *         row key ranges require the end key to be exclusive so this method
     *         will return a row key that is one after the row key represented
     *         by the end range time.
     */
    RowKey buildEndKey(String eventName, final RollUpBitMask rollUpBitMask, long rangeEndTime);

    default byte[] buildEndKeyBytes(final String eventName, final RollUpBitMask rollUpBitMask, final long rangeEndTime) {
        return buildEndKey(eventName, rollUpBitMask, rangeEndTime).asByteArray();
    }

    default byte[] buildEndKeyBytes(final String eventName, final RollUpBitMask rollUpBitMask) {
        //build a start key with any time value then copy it but with the highest possible
        //partial timestamp
        RowKey startKey = buildStartKey(eventName, rollUpBitMask, 0L);
        RowKey endKey = new RowKey(startKey.getTypeId(), startKey.getRollUpBitMask(), Bytes.toBytes(Integer.MAX_VALUE), Collections.emptyList());
        return endKey.asByteArray();
    }

    /**
     * Returns the event name part (as plain text) of a given row key object.
     * Uses a lookup on the UID table to resolve the row key's name byte array
     * into a string.
     *
     * @param rowKey
     *            The row key to extract the name from
     * @return The plain text event name string
     */
    String getNamePart(RowKey rowKey);

    /**
     * Converts the byte array representation of the row key time interval
     * number into an actual rounded time in millis (since the epoch)
     *
     * @param rowKey
     *            The row key object to extract the time from
     * @return the rounded (to the interval passed to this builder class)
     *         timestamp in millis since the epoch
     */
    long getPartialTimestamp(RowKey rowKey);

    /**
     * Returns a map of tags/values in plain text. The tag/value UIDs are
     * extracted from the passed row key and using the UID table are converted
     * into plain text form.
     *
     * @param rowKey
     *            The row key to extract it from
     * @return A map containing tags/values in plain text form
     */
    Map<String, String> getTagValuePairsAsMap(RowKey rowKey);

    // TODO Need to decide if we should return this as a list or a map
    List<StatisticTag> getTagValuePairsAsList(RowKey rowKey);

    /**
     * String to return a plain text string of the row key, making use of the
     * UID table lookups. Intended for debugging purposes.
     *
     * @param rowKey
     *            The RowKey object containing the byte arrays
     * @return Plain text string representation of the row key
     */
    String toPlainTextString(RowKey rowKey);

    TimeAgnosticRowKey buildTimeAgnosticRowKey(TimeAgnosticStatisticEvent timeAgnosticStatisticEvent);
}
