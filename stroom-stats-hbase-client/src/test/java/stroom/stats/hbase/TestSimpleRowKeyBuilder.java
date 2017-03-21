

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
import org.junit.Test;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.hbase.structure.CellQualifier;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestSimpleRowKeyBuilder {
    private final UniqueIdCache uniqueIdCache = new MockUniqueIdCache();

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSimpleRowKeyBuilder.class);

    @Test
    public void testBuildCellQualifier() {
        final EventStoreTimeIntervalEnum timeInterval = EventStoreTimeIntervalEnum.SECOND;

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, timeInterval);

        final String eventTimeString = "2009-01-01T10:11:12.134Z";
        final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

        final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

        final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

        assertEquals(1, rolledUpStatisticEvent.getPermutationCount());

        final CellQualifier cellQualifier = cellQualifiers.get(0);

        // should be rounded to the second with millis stripped off
        assertEquals("2009-01-01T10:11:12.000Z", DateUtil.createNormalDateTimeString(cellQualifier.getFullTimestamp()));

        final byte[] partialTimestamp = cellQualifier.getRowKey().getPartialTimestamp();

        final long keyTimestamp = Bytes.toInt(partialTimestamp) * timeInterval.rowKeyInterval();

        LOGGER.debug(DateUtil.createNormalDateTimeString(keyTimestamp));

        // the partial key timestamp should be rounded to the hour
        assertEquals("2009-01-01T10:00:00.000Z", DateUtil.createNormalDateTimeString(keyTimestamp));

        final long fullTimestamp = keyTimestamp +
                (cellQualifier.getColumnQualifier().getValue() * timeInterval.columnInterval());

        LOGGER.debug(DateUtil.createNormalDateTimeString(fullTimestamp));

        // the timestamp rebuilt from the row key and column should be rounded
        // to second
        assertEquals("2009-01-01T10:11:12.000Z", DateUtil.createNormalDateTimeString(fullTimestamp));

    }

    @Test
    public void testBuildMultipleCellQualifiers() {
        final EventStoreTimeIntervalEnum timeInterval = EventStoreTimeIntervalEnum.SECOND;

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, timeInterval);

        final String eventTimeString = "2009-01-01T10:11:12.134Z";
        final long eventTime = DateUtil.parseNormalDateTimeString(eventTimeString);

        final List<List<StatisticTag>> tagListPerms = new ArrayList<>();
        final List<StatisticTag> tagList = new ArrayList<StatisticTag>();
        tagList.add(new StatisticTag("Tag1", "Tag1Val"));
        tagList.add(new StatisticTag("Tag2", "Tag2Val"));
        tagListPerms.add(tagList);

        final List<StatisticTag> tagList2 = new ArrayList<StatisticTag>();
        tagList2.add(new StatisticTag("Tag1", "*"));
        tagList2.add(new StatisticTag("Tag2", "*"));
        tagListPerms.add(tagList2);

        final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", tagList, 1L);

        final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event, tagListPerms);

        final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

        assertEquals(2, rolledUpStatisticEvent.getPermutationCount());

        for (final CellQualifier cellQualifier : cellQualifiers) {
            // should be rounded to the second with millis stripped off
            assertEquals("2009-01-01T10:11:12.000Z",
                    DateUtil.createNormalDateTimeString(cellQualifier.getFullTimestamp()));

            final byte[] partialTimestamp = cellQualifier.getRowKey().getPartialTimestamp();

            final long keyTimestamp = Bytes.toInt(partialTimestamp) * timeInterval.rowKeyInterval();

            LOGGER.debug(DateUtil.createNormalDateTimeString(keyTimestamp));

            // the partial key timestamp should be rounded to the hour
            assertEquals("2009-01-01T10:00:00.000Z", DateUtil.createNormalDateTimeString(keyTimestamp));

            final long fullTimestamp = keyTimestamp +
                    (cellQualifier.getColumnQualifier().getValue() * timeInterval.columnInterval());

            LOGGER.debug(DateUtil.createNormalDateTimeString(fullTimestamp));

            // the timestamp rebuilt from the row key and column should be
            // rounded to second
            assertEquals("2009-01-01T10:11:12.000Z", DateUtil.createNormalDateTimeString(fullTimestamp));

        }

    }

    @Test
    public void testConvertCellQualifier() {
        EventStoreTimeIntervalEnum workingInterval = EventStoreTimeIntervalEnum.SECOND;

        final RowKeyBuilder rowKeyBuilder = new SimpleRowKeyBuilder(uniqueIdCache, workingInterval);

        final long eventTime = DateUtil.parseNormalDateTimeString("2009-01-01T10:11:12.134Z");

        final StatisticEvent event = new StatisticEvent(eventTime, "MyEvent", null, 1L);

        final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

        final List<CellQualifier> cellQualifiers = rowKeyBuilder.buildCellQualifiers(rolledUpStatisticEvent);

        assertEquals(1, rolledUpStatisticEvent.getPermutationCount());

        CellQualifier cellQualifier = cellQualifiers.get(0);

        // cellQualifier =
        // rowKeyBuilder.buildCellQualifier(cellQualifier.getRowKey(),
        // cellQualifier.getColumnQualifier());

        LOGGER.debug(DateUtil.createNormalDateTimeString(cellQualifier.getFullTimestamp()));

        // full timestamp should be original rounded to the second
        assertEquals(DateUtil.parseNormalDateTimeString("2009-01-01T10:11:12.000Z"), cellQualifier.getFullTimestamp());

        workingInterval = EventStoreTimeIntervalEnum.MINUTE;

        cellQualifier = SimpleRowKeyBuilder.convertCellQualifier(cellQualifier, workingInterval);

        assertEquals("2009-01-01T10:11:00.000Z", DateUtil.createNormalDateTimeString(cellQualifier.getFullTimestamp()));
        assertEquals("2009-01-01T10:11:00.000Z", DateUtil
                .createNormalDateTimeString(buildTimestampFromRowKeyAndColQual(workingInterval, cellQualifier)));

        workingInterval = EventStoreTimeIntervalEnum.HOUR;

        cellQualifier = SimpleRowKeyBuilder.convertCellQualifier(cellQualifier, workingInterval);

        assertEquals("2009-01-01T10:00:00.000Z", DateUtil.createNormalDateTimeString(cellQualifier.getFullTimestamp()));
        assertEquals("2009-01-01T10:00:00.000Z", DateUtil
                .createNormalDateTimeString(buildTimestampFromRowKeyAndColQual(workingInterval, cellQualifier)));

        workingInterval = EventStoreTimeIntervalEnum.DAY;

        cellQualifier = SimpleRowKeyBuilder.convertCellQualifier(cellQualifier, workingInterval);

        assertEquals("2009-01-01T00:00:00.000Z", DateUtil.createNormalDateTimeString(cellQualifier.getFullTimestamp()));
        assertEquals("2009-01-01T00:00:00.000Z", DateUtil
                .createNormalDateTimeString(buildTimestampFromRowKeyAndColQual(workingInterval, cellQualifier)));

    }

    private long buildTimestampFromRowKeyAndColQual(final EventStoreTimeIntervalEnum interval,
            final CellQualifier cellQualifier) {
        final long keyTimestamp = Bytes.toInt(cellQualifier.getRowKey().getPartialTimestamp())
                * interval.rowKeyInterval();
        final long fullTimestamp = keyTimestamp +
                (cellQualifier.getColumnQualifier().getValue() * interval.columnInterval());
        return fullTimestamp;
    }
}
