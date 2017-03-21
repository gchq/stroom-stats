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

package stroom.stats.streams;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.logging.LambdaLogger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A representation of the key portion of a statistic event. The Stat name and all
 * tags and values are held as UIDs to speed serialization and deserialization in kafka.
 * An instance can self serialize to a byte[]. The elements of the byte[] are:
 * <statName>[rollUp][interval][truncTime]<t1><v1>...<tN><vN>
 * Where each <...> is a UID
 *
 * The underlying UIDs are built on top of existing byte[]s. These byte[]s should not be mutated as StatKey is treated
 * as effectively immutable and caches its hashcode
 */
public class StatKey {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatKey.class);

    //Define all the array lengths and offsets for the various elements of the full stat key byte[]
    static int UID_ARRAY_LENGTH = UID.UID_ARRAY_LENGTH;
    static int STAT_NAME_PART_LENGTH = UID_ARRAY_LENGTH;
    static int STAT_NAME_PART_OFFSET = 0;
    static int ROLLUP_MASK_PART_LENGTH = RollUpBitMask.BYTE_VALUE_LENGTH;
    static int ROLLUP_MASK_PART_OFFSET = STAT_NAME_PART_OFFSET + STAT_NAME_PART_LENGTH;
    static int INTERVAL_PART_LENGTH = EventStoreTimeIntervalEnum.BYTE_VALUE_LENGTH;
    static int INTERVAL_PART_OFFSET = ROLLUP_MASK_PART_OFFSET + ROLLUP_MASK_PART_LENGTH;
    static int TIME_PART_LENGTH = Long.BYTES;
    static int TIME_PART_OFFSET = INTERVAL_PART_OFFSET + INTERVAL_PART_LENGTH;
    static int STATIC_PART_LENGTH = STAT_NAME_PART_LENGTH + ROLLUP_MASK_PART_LENGTH + INTERVAL_PART_LENGTH + TIME_PART_LENGTH;
    static int TAG_VALUE_PAIR_LENGTH = UID_ARRAY_LENGTH * 2;
    static int TAG_VALUE_PAIRS_OFFSET = TIME_PART_OFFSET + TIME_PART_LENGTH;

    //    private final  byte[] key;
    private final UID statName;
    private final RollUpBitMask rollupMask;
    private EventStoreTimeIntervalEnum interval;
    private long timeMs;
    private final List<TagValue> tagValues;
    private int hashCode;

    public StatKey(final UID statName,
                   final RollUpBitMask rollupMask,
                   final EventStoreTimeIntervalEnum interval,
                   final long timeMs,
                   final List<TagValue> tagValues) {

        Preconditions.checkNotNull(statName);
        Preconditions.checkNotNull(rollupMask);
        Preconditions.checkNotNull(interval);
        Preconditions.checkNotNull(tagValues);

        this.statName = statName;
        this.rollupMask = rollupMask;
        this.interval = interval;
        this.timeMs = interval.truncateTimeToColumnInterval(timeMs);
        this.tagValues = tagValues;
        this.hashCode = buildHashCode();
    }


    public StatKey(final UID statName,
                   final RollUpBitMask rollupMask,
                   final EventStoreTimeIntervalEnum interval,
                   final long timeMs,
                   final TagValue... tagValues) {

       this(statName, rollupMask, interval, timeMs, Arrays.asList(tagValues));
    }
    /**
     * Spawns a new {@link StatKey} based on the values of this but with some of
     * the tag values rolled up and a new {@link RollUpBitMask}. The new tag values
     * are defined by the new RollUpBitMask.
     *
     * This is a shallow clone, so has references to the sames underlying objects as
     * the original instance.
     *
     * Care should be taken to only call this on an instance that has no tags rolled up
     * else the roll up behaviour will be cumulative rather than absolute.
     */
    public StatKey cloneAndRollUpTags(final RollUpBitMask newRollUpBitMask, final UID rolledUpValue) {
        Preconditions.checkNotNull(newRollUpBitMask);
        Preconditions.checkNotNull(rolledUpValue);
        int tagCount = tagValues.size();

        List<Boolean> rolledUpTags = newRollUpBitMask.getBooleanMask(tagCount);

        List<TagValue> newTagValues = new ArrayList<>(tagCount);

        for (int i = 0; i < tagCount; i++) {
            if (rolledUpTags.get(i)){
                newTagValues.add(tagValues.get(i).cloneAndRollUp(rolledUpValue));
            } else {
                newTagValues.add(tagValues.get(i));
            }
        }
        return new StatKey(statName, newRollUpBitMask, interval, timeMs, newTagValues);
    }

    /**
     * Shallow copy of this except the interval is changed for the next biggest. Will throw a {@link RuntimeException}
     * if it is already the biggest.
     */
    public StatKey cloneAndIncrementInterval() {
        EventStoreTimeIntervalEnum newInterval = EventStoreTimeIntervalHelper.getNextBiggest(interval).orElseThrow(() ->
                new RuntimeException(String.format("Cannot increment current interval %s as it is the biggest interval", interval.toString())));
        return new StatKey(statName, rollupMask, newInterval, timeMs, tagValues);
    }

    /**
     * Shallow copy of this except the interval is changed for the next biggest. Will throw a {@link RuntimeException}
     * if it is already the biggest.
     */
    public StatKey cloneAndChangeInterval(EventStoreTimeIntervalEnum newInterval) {
        Preconditions.checkNotNull(newInterval);
        return new StatKey(statName, rollupMask, newInterval, timeMs, tagValues);
    }

    /**
     * Shallow copy of this instance except the timeMs value is truncated down to the nearest interval
     */
    public StatKey cloneAndTruncateTimeToInterval() {
        long newTimeMs = interval.truncateTimeToColumnInterval(timeMs);
        return new StatKey(statName, rollupMask, interval, newTimeMs, tagValues);
    }

    /**
     * Shallow copy of this instance except the timeMs value is truncated down to the new interval
     * and the interval is changed to the new interval
     */
    public StatKey cloneAndTruncateTimeToInterval(final EventStoreTimeIntervalEnum newInterval) {
        Preconditions.checkArgument(newInterval.compareTo(this.interval) > 1,
                "newInterval %s should be larger than interval %s", interval, newInterval);
        long newTimeMs = interval.truncateTimeToColumnInterval(timeMs);
        return new StatKey(statName, rollupMask, newInterval, newTimeMs, tagValues);
    }

    public byte[] getBytes() {
        int length = STATIC_PART_LENGTH + (tagValues.size() * TAG_VALUE_PAIR_LENGTH);
        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        byteBuffer.put(statName.getUidBytes());
        byteBuffer.put(rollupMask.asBytes());
        byteBuffer.put(interval.getByteVal());
        byteBuffer.put(Bytes.toBytes(timeMs));
        tagValues.forEach(tagValue -> byteBuffer.put(tagValue.getBytes()));
        return byteBuffer.array();
    }

    public static StatKey fromBytes(final byte[] bytes) {
//        byte[] statName = Arrays.copyOfRange(bytes, STAT_NAME_PART_OFFSET, STAT_NAME_PART_LENGTH);
        UID uid = UID.from(bytes, STAT_NAME_PART_OFFSET);
        RollUpBitMask rollUpBitMask = RollUpBitMask.fromBytes(bytes, ROLLUP_MASK_PART_OFFSET);
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.fromBytes(bytes, INTERVAL_PART_OFFSET);
        long timeMs = Bytes.toLong(bytes, TIME_PART_OFFSET);
        List<TagValue> tagValues = getTagValues(bytes);

        try {
            StatKey statKey = new StatKey(uid, rollUpBitMask, interval, timeMs, tagValues);
            LOGGER.trace(() -> String.format("De-serializing bytes %s to StatKey %s", ByteArrayUtils.byteArrayToHex(bytes), statKey));
            return statKey;
        } catch (Exception e) {
            LOGGER.error("Error de-serializing a StatKey from bytes {}", ByteArrayUtils.byteArrayToHex(bytes));
            throw e;
        }
    }

    public UID getStatName() {
        return statName;
    }

//    public byte[] getStatNameCopy() {
//        ByteBuffer byteBuffer = statName.duplicate();
//        byteBuffer.position(0);
//        byteBuffer.limit(STAT_NAME_PART_LENGTH);
//        byte[] copy = new byte[STAT_NAME_PART_LENGTH];
//        byteBuffer.get(copy);
//        return copy;
//    }

    public RollUpBitMask getRollupMask() {
        return rollupMask;
    }

    public EventStoreTimeIntervalEnum getInterval() {
        return interval;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public List<TagValue> getTagValues() {
        return tagValues;
    }

    public boolean equalsIntervalPart(EventStoreTimeIntervalEnum other) {
        return interval.equals(other);
    }

    public static List<TagValue> getTagValues(final byte[] bytes) {
        List<TagValue> tagValues = new ArrayList<>();
        int keyLength = bytes.length;
        int tagValuesLength = keyLength - STATIC_PART_LENGTH;
        if (tagValuesLength > 0) {
            int offset = TAG_VALUE_PAIRS_OFFSET;
            while (offset < keyLength) {
                tagValues.add(new TagValue(bytes, offset));
                offset += TAG_VALUE_PAIR_LENGTH;
            }
        }
        return tagValues;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final StatKey statKey = (StatKey) o;

        if (timeMs != statKey.timeMs) return false;
        if (!statName.equals(statKey.statName)) return false;
        if (!rollupMask.equals(statKey.rollupMask)) return false;
        if (interval != statKey.interval) return false;
        return tagValues.equals(statKey.tagValues);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
    public int buildHashCode() {
        int result = statName.hashCode();
        result = 31 * result + rollupMask.hashCode();
        result = 31 * result + interval.hashCode();
        result = 31 * result + (int) (timeMs ^ (timeMs >>> 32));
        result = 31 * result + tagValues.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StatKey{" +
//                "statName=" + ByteArrayUtils.byteBufferToHex(statName) +
                "statName=" + statName +
//                ", rollupMask=" + rollupMask +
                ", interval=" + interval +
                ", timeMs=" + timeMs +
                ", tagValues=" + tagValues +
                '}';
    }
}
