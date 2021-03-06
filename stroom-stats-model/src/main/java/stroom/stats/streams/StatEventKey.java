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
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.util.bytes.ByteArrayUtils;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.logging.LambdaLogger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A representation of the key portion of a statistic event. The Stat name and all
 * tags and values are held as UIDs to speed serialization and deserialization in kafka.
 * An instance can self serialize to a byte[]. The elements of the byte[] are:
 * <statUuid>[rollUp][interval][truncTime]<t1><v1>...<tN><vN>
 * Where each <...> is a UID
 *
 * The underlying UIDs are built on top of existing byte[]s. These byte[]s should not be mutated as StatEventKey is treated
 * as effectively immutable and caches its hashcode
 *
 * The time element in a {@link StatEventKey} will ALWAYS be truncated down to the nearest {@link EventStoreTimeIntervalEnum}.
 * This truncation will happen in the public constructors and in certain clone operations
 */
public class StatEventKey implements Comparable<StatEventKey> {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatEventKey.class);

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

    public static final Comparator<StatEventKey> COMPARATOR = Comparator
            .comparing(StatEventKey::getStatUuid)
            .thenComparing(StatEventKey::getInterval)
            .thenComparing(StatEventKey::getRollupMask)
            .thenComparingLong(StatEventKey::getTimeMs)
            .thenComparing(StatEventKey::getTagValues, TagValue.TAG_VALUES_COMPARATOR);


    private final UID statUuid;
    private final RollUpBitMask rollupMask;
    private EventStoreTimeIntervalEnum interval;
    private long timeMs;
    private final List<TagValue> tagValues;

    private volatile int hashCode = 0;

    private enum TimeTruncation {
        TRUNCATE,
        DONT_TRUNCATE
    }

    private StatEventKey(final UID statUuid,
                         final RollUpBitMask rollupMask,
                         final EventStoreTimeIntervalEnum interval,
                         final long timeMs,
                         final List<TagValue> tagValues,
                         final TimeTruncation timeTruncation) {

        Preconditions.checkNotNull(statUuid);
        Preconditions.checkNotNull(rollupMask);
        Preconditions.checkNotNull(interval);
        Preconditions.checkNotNull(tagValues);

        this.statUuid = statUuid;
        this.rollupMask = rollupMask;
        this.interval = interval;
        this.tagValues = tagValues;
        switch (timeTruncation) {
            case TRUNCATE:
                this.timeMs = interval.truncateTimeToColumnInterval(timeMs);
                break;
            case DONT_TRUNCATE:
                this.timeMs = timeMs;
                break;
            default:
                throw new IllegalArgumentException(String.format("Unexpected value for timeTruncation: %s", timeTruncation));
        }
    }

    public StatEventKey(final UID statUuid,
                        final RollUpBitMask rollupMask,
                        final EventStoreTimeIntervalEnum interval,
                        final long timeMs,
                        final List<TagValue> tagValues) {

        //public constructor so ensure the time is truncated to the time interval
        this(statUuid, rollupMask, interval, timeMs, tagValues, TimeTruncation.TRUNCATE);
    }


    public StatEventKey(final UID statUuid,
                        final RollUpBitMask rollupMask,
                        final EventStoreTimeIntervalEnum interval,
                        final long timeMs,
                        final TagValue... tagValues) {

        //public constructor so ensure the time is truncated to the time interval
       this(statUuid, rollupMask, interval, timeMs, Arrays.asList(tagValues), TimeTruncation.TRUNCATE);
    }

    /**
     * Spawns a new {@link StatEventKey} based on the values of this but with some of
     * the tag values rolled up and a new {@link RollUpBitMask}. The new tag values
     * are defined by the new RollUpBitMask.
     *
     * This is a shallow clone, so has references to the sames underlying objects as
     * the original instance.
     *
     * Care should be taken to only call this on an instance that has no tags rolled up
     * else the roll up behaviour will be cumulative rather than absolute.
     */
    public StatEventKey cloneAndRollUpTags(final RollUpBitMask newRollUpBitMask, final UID rolledUpValue) {
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
        //time of the new key is unchanged from this so don't truncate
        return new StatEventKey(statUuid, newRollUpBitMask, interval, timeMs, newTagValues, TimeTruncation.DONT_TRUNCATE);
    }

    /**
     * Shallow copy of this except the interval is changed for the next biggest. Will throw a {@link RuntimeException}
     * if it is already the biggest.
     */
    public StatEventKey cloneAndChangeInterval(EventStoreTimeIntervalEnum newInterval) {
        Preconditions.checkNotNull(newInterval);
        //truncate the time in the new statKey down to the new interval
        return new StatEventKey(statUuid, rollupMask, newInterval, timeMs, tagValues, TimeTruncation.TRUNCATE);
    }

    public byte[] getBytes() {
        int length = STATIC_PART_LENGTH + (tagValues.size() * TAG_VALUE_PAIR_LENGTH);
        ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        byteBuffer.put(statUuid.getUidBytes());
        byteBuffer.put(rollupMask.asBytes());
        byteBuffer.put(interval.getByteVal());
        byteBuffer.put(Bytes.toBytes(timeMs));
        tagValues.forEach(tagValue -> byteBuffer.put(tagValue.getBytes()));
        return byteBuffer.array();
    }

    public static StatEventKey fromBytes(final byte[] bytes) {
        UID statName = UID.from(bytes, STAT_NAME_PART_OFFSET);
        RollUpBitMask rollUpBitMask = RollUpBitMask.fromBytes(bytes, ROLLUP_MASK_PART_OFFSET);
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.fromBytes(bytes, INTERVAL_PART_OFFSET);
        long timeMs = Bytes.toLong(bytes, TIME_PART_OFFSET);
        List<TagValue> tagValues = getTagValues(bytes);

        try {
            //de-serializing so leave time as in its byte form
            StatEventKey statEventKey = new StatEventKey(statName, rollUpBitMask, interval, timeMs, tagValues, TimeTruncation.DONT_TRUNCATE);
            LOGGER.trace(() -> String.format("De-serializing bytes %s to StatEventKey %s", ByteArrayUtils.byteArrayToHex(bytes), statEventKey));
            return statEventKey;
        } catch (Exception e) {
            LOGGER.error("Error de-serializing a StatEventKey from bytes {}", ByteArrayUtils.byteArrayToHex(bytes));
            throw e;
        }
    }

    public UID getStatUuid() {
        return statUuid;
    }

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

        final StatEventKey statEventKey = (StatEventKey) o;

        if (timeMs != statEventKey.timeMs) return false;
        if (!statUuid.equals(statEventKey.statUuid)) return false;
        if (!rollupMask.equals(statEventKey.rollupMask)) return false;
        if (interval != statEventKey.interval) return false;
        return tagValues.equals(statEventKey.tagValues);
    }

    @Override
    public int hashCode() {
        //instance is immutable so lazily cache the hashcode for speed
        if (hashCode == 0) {
            hashCode = buildHashCode();
        }
        return hashCode;
    }

    public int buildHashCode() {
        int result = statUuid.hashCode();
        result = 31 * result + rollupMask.hashCode();
        result = 31 * result + interval.hashCode();
        result = 31 * result + Long.hashCode(timeMs);
        result = 31 * result + tagValues.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "StatEventKey{" +
                "statUuid=" + statUuid +
                ", rollupMask=" + rollupMask +
                ", interval=" + interval +
                ", timeMs=" + timeMs +
                ", tagValues=" + tagValues +
                '}';
    }

    @Override
    public int compareTo(final StatEventKey that) {
        return COMPARATOR.compare(this, that);
    }
}
