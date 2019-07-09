

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

package stroom.stats.shared;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Enum used to hold the intervals used in storing the event stats. The time of
 * the event is rounded to two levels, the row key level and the column level
 * within each row. The column interval is the finest granularity available. If
 * the column interval is too small relative to the row key interval then you
 * will end up with too many columns.
 */
public enum EventStoreTimeIntervalEnum {
    // IMPORTANT - Each interval must be exactly divisible into the next
    // largest. This is to allow us to roll up the
    // times from one store to the next without knowing the original exact time

    // Hourly row key with 3600 1sec column intervals
    SECOND(3_600_000L, 1_000L, "Second", "s", EventStoreTimeIntervalEnum::truncateTimeToInterval),
    // Daily row key with 1440 1min column intervals
    MINUTE(86_400_000L, 60_000L, "Minute", "m", EventStoreTimeIntervalEnum::truncateTimeToInterval),
    // Four Weekly row key with 672 1hour column intervals
    HOUR(2_419_200_000L, 3_600_000L, "Hour", "h", EventStoreTimeIntervalEnum::truncateTimeToInterval),
    // Fifty two Weekly row key with 364 1day column intervals
    DAY(31_449_600_000L, 86_400_000L, "Day", "d", EventStoreTimeIntervalEnum::truncateTimeToInterval),
    // Single row with one cell representing all time past and present
    FOREVER(Long.MAX_VALUE, Long.MAX_VALUE, "Forever", "f", EventStoreTimeIntervalEnum::truncateToForever);

    public static final int BYTE_VALUE_LENGTH = Long.BYTES;
    public static final EventStoreTimeIntervalEnum LARGEST_INTERVAL;
    public static final EventStoreTimeIntervalEnum SMALLEST_INTERVAL;
    public static final Comparator<EventStoreTimeIntervalEnum> comparator =
            Comparator.comparingLong(EventStoreTimeIntervalEnum::columnInterval);

    private static final TreeSet<EventStoreTimeIntervalEnum> reverseSortedSet = new TreeSet<>(comparator.reversed());
    private final long rowKeyInterval;
    private final long columnInterval;
    private final String longName;
    private final String shortName;
    private final byte[] byteVal;
    private final TruncateFunction truncateFunction;

    private static final Map<Long, EventStoreTimeIntervalEnum> fromColumnIntervalMap = new HashMap<>();
    private static final Map<String, EventStoreTimeIntervalEnum> fromShortnameMap = new HashMap<>();

    static {
        for (final EventStoreTimeIntervalEnum interval : values()) {
            fromColumnIntervalMap.put(interval.columnInterval, interval);
            fromShortnameMap.put(interval.shortName, interval);
        }
        reverseSortedSet.addAll(Arrays.asList(EventStoreTimeIntervalEnum.values()));
        //set is reverse sorted
        LARGEST_INTERVAL = EventStoreTimeIntervalEnum.reverseSortedSet.first();
        SMALLEST_INTERVAL = EventStoreTimeIntervalEnum.reverseSortedSet.last();
    }

    /**
     * Constructor
     *
     * @param rowKeyInterval The time interval in milliseconds for the row key
     * @param columnInterval The time interval in milliseconds for the column qualifier
     * @param longName       The Human readable name for the interval
     * @param shortName      The short name for the interval, used in the hbase table name
     */
    EventStoreTimeIntervalEnum(final long rowKeyInterval,
                               final long columnInterval,
                               final String longName,
                               final String shortName,
                               final TruncateFunction truncateFunction) {
        this.rowKeyInterval = rowKeyInterval;
        this.columnInterval = columnInterval;
        this.longName = longName;
        this.shortName = shortName;
        this.byteVal = Bytes.toBytes(columnInterval);
        this.truncateFunction = truncateFunction;
    }

    /**
     * Works out what the next biggest (i.e. more coarse grained) interval is
     * after this object. If this is the biggest then it will return an empty optional
     *
     * @return The next biggest interval or an empty Optional
     */
    public static Optional<EventStoreTimeIntervalEnum> getNextBiggest(final EventStoreTimeIntervalEnum interval) {
        // set is in reverse order so use lower to get the next biggest
        return Optional.ofNullable(reverseSortedSet.lower(interval));
    }

    /**
     * Works out what the next smallest (i.e. more coarse grained) interval is
     * after this object. If this is the smallest then it will return an empty optional
     *
     * @return The next smallest interval or an empty Optional
     */
    public static Optional<EventStoreTimeIntervalEnum> getNextSmallest(final EventStoreTimeIntervalEnum interval) {
        // set is in reverse order so use lower to get the next smallest
        return Optional.ofNullable(reverseSortedSet.higher(interval));
    }

    public static EventStoreTimeIntervalEnum getSmallestInterval() {
        // set is reverse sorted
        return SMALLEST_INTERVAL;
    }

    public static EventStoreTimeIntervalEnum getLargestInterval() {
        // set is reverse sorted
        return LARGEST_INTERVAL;
    }

    public static SortedSet<EventStoreTimeIntervalEnum> getReverseSortedSet() {
        return reverseSortedSet;
    }

    private static interface TruncateFunction {
        long truncate(final long timestampMillis, final long intervalMillis);
    }


    public static EventStoreTimeIntervalEnum fromColumnInterval(final long columnInterval) {
        try {
            return fromColumnIntervalMap.get(columnInterval);
        } catch (final Exception e) {
            throw new IllegalArgumentException(
                    "The provided columnInterval [" + columnInterval + "] is not a valid event store interval", e);
        }
    }

    public static EventStoreTimeIntervalEnum fromShortName(final String shortName) {
        try {
            return fromShortnameMap.get(shortName);
        } catch (final Exception e) {
            throw new IllegalArgumentException(
                    "The provided shortName [" + shortName + "] is not a valid name for an event store", e);
        }
    }

    public static EventStoreTimeIntervalEnum fromBytes(final byte[] bytes) {
        return EventStoreTimeIntervalEnum.fromColumnInterval(Bytes.toLong(bytes));
    }

    public static EventStoreTimeIntervalEnum fromBytes(final byte[] bytes, final int offset) {
        return EventStoreTimeIntervalEnum.fromColumnInterval(Bytes.toLong(bytes, offset));
    }

    /**
     * Standard function for truncating a time down to an interval
     *
     * @return The time at the start of the interval bucket
     */
    private static long truncateTimeToInterval(final long timestampMillis, final long intervalMillis) {
        return ((long) (timestampMillis / intervalMillis)) * intervalMillis;
    }

    /**
     * Special case for truncating a time to our Forever interval, else we would get a
     * divide by zero error
     *
     * @return The time at the start of the interval bucket
     */
    private static long truncateToForever(final long timestampMillis, final long intervalMillis) {
        //all times in the forever interval are truncated down to the epoch
        return 0;
    }

    /**
     * @return the time interval used for the row key in milliseconds
     */
    public long rowKeyInterval() {
        return this.rowKeyInterval;
    }

    /**
     * @return the time interval used for the column qualifier in milliseconds
     */
    public long columnInterval() {
        return this.columnInterval;
    }

    /**
     * @return the readable name of the row key interval, e.g 'Second'
     */
    public String longName() {
        return this.longName;
    }

    /**
     * @return the short code name of the row key interval, e.g 's'
     */
    public String shortName() {
        return this.shortName;
    }

    public byte[] getByteVal() {
        return this.byteVal;
    }

    public String getDisplayValue() {
        return this.longName;
    }

    public String getRowKeyIntervalAsString() {
        return timeIntervalToString(rowKeyInterval);
    }

    private String timeIntervalToString(final long timeMs) {
        final double seconds = timeMs / 1000;
        final double mintes = timeMs / (1000 * 60);
        final double hours = timeMs / (1000 * 60 * 60);
        final double days = timeMs / (1000 * 60 * 60 * 24);
        final double weeks = timeMs / (1000 * 60 * 60 * 24 * 7);

        final StringBuilder sb = new StringBuilder();
        sb.append("seconds: ");
        sb.append(seconds);
        sb.append(",  mintes: ");
        sb.append(mintes);
        sb.append(",  hours: ");
        sb.append(hours);
        sb.append(",  days: ");
        sb.append(days);
        sb.append(",  weeks: ");
        sb.append(weeks);

        return sb.toString();
    }


    public long truncateTimeToColumnInterval(final long timestampMillis) {
        return truncateFunction.truncate(timestampMillis, this.columnInterval);
    }

    public long truncateTimeToRowKeyInterval(final long timestampMillis) {
        return truncateFunction.truncate(timestampMillis, this.rowKeyInterval);
    }


//    public static class InvalidTimeIntervalException extends RuntimeException {
//        private static final long serialVersionUID = -7383690663978635032L;
//
//        public InvalidTimeIntervalException(final String msg) {
//            super(msg);
//        }
//    }

    public boolean isLargerThan(final EventStoreTimeIntervalEnum other) {
        return this.columnInterval > other.columnInterval;
    }

    public boolean isSmallerThan(final EventStoreTimeIntervalEnum other) {
        return this.columnInterval < other.columnInterval;
    }
}
