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
