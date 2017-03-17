

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

import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class with static helper methods for the {@link EventStoreTimeIntervalEnum}.
 * The reason these methods don't live in the {@link EventStoreTimeIntervalEnum}
 * is that that class has to live in stroom-core-shared and GWT doesn't like some
 * of the ConcurrentHashMaps
 */
public class EventStoreTimeIntervalHelper {
    private static Map<Long, List<EventStoreTimeIntervalEnum>> matchingIntervalsCache = new ConcurrentHashMap<>();
    private static Map<Long, EventStoreTimeIntervalEnum> matchingIntervalCache = new ConcurrentHashMap<>();
    private static final EventStoreTimeIntervalEnum LARGEST_INTERVAL;
    private static final EventStoreTimeIntervalEnum SMALLEST_INTERVAL;

    private static final TreeSet<EventStoreTimeIntervalEnum> sortedSet = new TreeSet<>(
            new EventStoreTimeIntervalEnumComparator());

    static {
        sortedSet.addAll(Arrays.asList(EventStoreTimeIntervalEnum.values()));
        //set is reverse sorted
        LARGEST_INTERVAL = sortedSet.first();
        SMALLEST_INTERVAL = sortedSet.last();
    }

    private EventStoreTimeIntervalHelper() {
        // Only static methods
    }

    /**
     * Method to return a list of {@link EventStoreTimeIntervalHelper} based on
     * the passed interval value. It will return the enums with column intervals
     * greater than or equal to the passed value. If the passed value lies
     * between the column intervals of two enums then both, plus all greater
     * will be returned.
     *
     * @param smallestTimeInterval
     *            time interval in milliseconds
     * @return The enums that best fit the pass time interval. See method
     *         description.
     */
    public static List<EventStoreTimeIntervalEnum> getMatchingIntervals(final long smallestTimeInterval) {
        // look up the interval in the matchCache. This function will always
        // return the same output for the same input
        // so the output can be cached indefinitely to save repeating this logic
        // on every put operation
        List<EventStoreTimeIntervalEnum> bestFitList = matchingIntervalsCache.get(new Long(smallestTimeInterval));

        if (bestFitList == null) {
            // not found in cache so work it out
            // there is a risk here that multiple threads will both see it as
            // null and compute the result but as teh
            // result will be same it is a very minor inefficiency

            bestFitList = new ArrayList<>();

            // set up a reverse sorted list of enums, sorted on the
            // columnInterval value
            final List<EventStoreTimeIntervalEnum> reverseSortedTimeIntervals = Arrays
                    .asList(EventStoreTimeIntervalEnum.values());

            final Comparator<EventStoreTimeIntervalEnum> comparator = new EventStoreTimeIntervalEnumComparator();
            Collections.sort(reverseSortedTimeIntervals, comparator);

            if (smallestTimeInterval > reverseSortedTimeIntervals.get(0).columnInterval())
                throw new InvalidTimeIntervalException(
                        "Supplied time interval is larger than any of the event stores [" + smallestTimeInterval + "]");

            // loop through the list of enums, biggest first
            for (final EventStoreTimeIntervalEnum timeIntervalEnum : reverseSortedTimeIntervals) {
                if (smallestTimeInterval == timeIntervalEnum.columnInterval()) {
                    // exact match so add it to the list and stop
                    bestFitList.add(timeIntervalEnum);
                    break;
                } else if (smallestTimeInterval < timeIntervalEnum.columnInterval()) {
                    bestFitList.add(timeIntervalEnum);
                } else {
                    // smallestTimeInterval lies between the last enum and this
                    // so include this one then stop
                    bestFitList.add(timeIntervalEnum);
                    break;
                }
            }

            // add it into the cache for others to use
            matchingIntervalsCache.put(smallestTimeInterval, bestFitList);
        }

        if (bestFitList.size() == 0) {
            throw new RuntimeException(
                    "Unable to find any matching time intervals for a time interval of " + smallestTimeInterval + "ms");
        }

        return bestFitList;
    }

    /**
     * Method to return a {@link EventStoreTimeIntervalHelper} that matches the
     * passed time interval. If there is no exact match then it will return the
     * closest enum with a interval lower than the passed value.
     *
     * @param smallestTimeInterval
     *            time interval in milliseconds
     * @return The enums that best fit the pass time interval. See method
     *         description.
     */
    public static EventStoreTimeIntervalEnum getMatchingInterval(final long smallestTimeInterval) {
        EventStoreTimeIntervalEnum matchingInterval = matchingIntervalCache.get(smallestTimeInterval);

        if (matchingInterval == null) {
            final List<EventStoreTimeIntervalEnum> intervals = getMatchingIntervals(smallestTimeInterval);
            // the result of getMatchingIntervals is a sorted list of intervals
            // , biggest first, so we want
            // the smallest ie. last one
            matchingInterval = intervals.get(intervals.size() - 1);
        }

        return matchingInterval;
    }

    /**
     * Based on a time period and a desired maximum number of time intervals
     * this will return the best interval size to use. E.g a 300s period with a
     * desired max interval count of 100 would give a interval size of 3s so
     * this would return the next biggest interval size (if not matching
     * exactly), i.e. the MINUTE interval object.
     *
     * @param searchPeriodMillis
     *            The duration in millis that the search is over.
     * @param desiredMaxIntervalsInPeriod
     *            The desired maximum number of time intervals in the period.
     * @return The {@link EventStoreTimeIntervalHelper} object that best matches
     *         the arguments supplied
     */
    public static EventStoreTimeIntervalEnum getBestFit(final long searchPeriodMillis,
            final int desiredMaxIntervalsInPeriod) {
        if (searchPeriodMillis == 0) {
            throw new IllegalArgumentException("searchPeriodMillis cannot be zero");
        }

        if (desiredMaxIntervalsInPeriod == 0) {
            throw new IllegalArgumentException("desiredMaxIntervalsInPeriod cannot be zero");
        }

        final long desiredIntervalMillis = searchPeriodMillis / desiredMaxIntervalsInPeriod;

        EventStoreTimeIntervalEnum bestFit = sortedSet.first();

        for (final EventStoreTimeIntervalEnum interval : sortedSet) {
            if (interval.columnInterval() < desiredIntervalMillis) {
                break;
            } else {
                bestFit = interval;
            }
        }

        return bestFit;
    }

    /**
     * Works out what the next biggest (i.e. more coarse grained) interval is
     * after this object. If this is the biggest then it will return an empty optional
     *
     * @return The next biggest interval or an empty Optional
     */
    public static Optional<EventStoreTimeIntervalEnum> getNextBiggest(final EventStoreTimeIntervalEnum interval) {
        // set is in reverse order so use lower to get the next biggest
        return Optional.ofNullable(sortedSet.lower(interval));
    }

    public static EventStoreTimeIntervalEnum getSmallestInterval() {
        // set is reverse sorted
        return SMALLEST_INTERVAL;
    }

    public static EventStoreTimeIntervalEnum getLargestInterval() {
        // set is reverse sorted
        return LARGEST_INTERVAL;
    }

    private static class EventStoreTimeIntervalEnumComparator implements Comparator<EventStoreTimeIntervalEnum> {
        @Override
        public int compare(final EventStoreTimeIntervalEnum interval1, final EventStoreTimeIntervalEnum interval2) {
            // sort them in reverse order, biggest first
            return -((Long) interval1.columnInterval()).compareTo(interval2.columnInterval());
        }
    }

    public static class InvalidTimeIntervalException extends RuntimeException {
        private static final long serialVersionUID = -7383690663978635032L;

        public InvalidTimeIntervalException(final String msg) {
            super(msg);
        }
    }
}
