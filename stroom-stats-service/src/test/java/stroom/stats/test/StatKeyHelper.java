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

package stroom.stats.test;

import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.uid.UID;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.TagValue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class StatKeyHelper {

    static UID statNameUid = UID.from(new byte[] {9,0,0,1});
    static RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
    static EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
    static LocalDateTime time = LocalDateTime.of(2016, 2, 22, 23, 55, 40);

    private StatKeyHelper() {
    }

    public static StatKey buildStatKey(final LocalDateTime time, final EventStoreTimeIntervalEnum interval) {
        long timeMs = time.toInstant(ZoneOffset.UTC).toEpochMilli();
        List<TagValue> tagValues = new ArrayList<>();
        tagValues.add(new TagValue(UID.from(new byte[] {9,0,1,1}), UID.from(new byte[] {9,0,1,1})));
        tagValues.add(new TagValue(UID.from(new byte[] {9,0,2,1}), UID.from(new byte[] {9,0,2,2})));
        tagValues.add(new TagValue(UID.from(new byte[] {9,0,3,1}), UID.from(new byte[] {9,0,3,2})));

        return new StatKey(statNameUid, rollUpBitMask, interval, timeMs, tagValues);
    }
}
