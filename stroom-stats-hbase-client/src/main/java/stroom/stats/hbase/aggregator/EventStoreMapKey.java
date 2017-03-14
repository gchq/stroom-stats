

/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.hbase.aggregator;

import com.google.common.base.Preconditions;
import stroom.stats.api.StatisticType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Holder class to hold the ID (could be a thread ID or some other identifier)
 * and time interval enum that make up the key to the in memory event store
 * maps.
 *
 * Implements Delayed so that we can track stores that have timed out by looking
 * at the expire time on the key. The expire time is reset-able for when new
 * event store instances are put to the map.
 */
public class EventStoreMapKey implements Delayed {
    private final StatisticType statisticType;
    private final long id;
    private final EventStoreTimeIntervalEnum timeInterval;
    private static final TimeUnit INTERNAL_WORKING_TIME_UNIT = TimeUnit.MILLISECONDS;

    private static final Logger LOGGER = LoggerFactory.getLogger(EventStoreMapKey.class);

    // this is the absolute time that the event store will be considered to be
    // timed out, ie. ready for a scheduled
    // flush
    private volatile long expiredTimeMillis;

    /**
     * Constructor.
     *
     * @param statisticType
     *            i.e. COUNT|VALUE
     * @param id
     *            An ID value that allows there to be one or more stores per
     *            stat type and interval, primarily used to allow multiple
     *            threads to work on a map in isolation.
     * @param timeInterval
     *            The time interval associated with the map, e.g. SECONDS,
     *            HOURS, etc.
     * @param timeOutDelay
     *            The time delta before the store associated with this key is
     *            deemed to have timed out and needs flushing
     * @param timeUnit
     *            The time unit of timeOutDelay
     */
    public EventStoreMapKey(final StatisticType statisticType, final long id,
            final EventStoreTimeIntervalEnum timeInterval, final long timeOutDelay, final TimeUnit timeUnit) {
        this.statisticType = statisticType;
        this.id = id;
        this.timeInterval = timeInterval;
        resetDelay(timeOutDelay, timeUnit);
    }

    /**
     * @return A copy of this object
     */
    public EventStoreMapKey copy() {
        return new EventStoreMapKey(statisticType, id, timeInterval, (expiredTimeMillis - System.currentTimeMillis()),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Mechanism to reset the delay in place on this key object. Note this
     * should not be used to change the delay of an object already placed on a
     * delay queue but instead to change the delay before putting on a delay
     * queue.
     *
     * @param timeOutDelay
     *            The delay time required
     * @param timeUnit
     *            The time unit of timeOutDelay
     */
    void resetDelay(final long timeOutDelay, final TimeUnit timeUnit) {
        final long newDelayMillis = INTERNAL_WORKING_TIME_UNIT.convert(timeOutDelay, timeUnit);

        LOGGER.trace("resetDelay called with old timeOutDelay: {}, newtimeOutDelay {}", expiredTimeMillis,
                newDelayMillis);

        this.expiredTimeMillis = System.currentTimeMillis() + newDelayMillis;
    }

    public StatisticType getStatisticType() {
        return this.statisticType;
    }

    public EventStoreTimeIntervalEnum getTimeInterval() {
        return timeInterval;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        result = prime * result + ((statisticType == null) ? 0 : statisticType.hashCode());
        result = prime * result + ((timeInterval == null) ? 0 : timeInterval.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof EventStoreMapKey))
            return false;
        final EventStoreMapKey other = (EventStoreMapKey) obj;
        if (id != other.id)
            return false;
        if (statisticType != other.statisticType)
            return false;
        if (timeInterval != other.timeInterval)
            return false;
        return true;
    }

    @Override
    public long getDelay(final TimeUnit unit) {
        Preconditions.checkNotNull(unit);
        return unit.convert((this.expiredTimeMillis - System.currentTimeMillis()), INTERNAL_WORKING_TIME_UNIT);
    }

    @Override
    public int compareTo(final Delayed delayed) {
        Preconditions.checkNotNull(delayed);
        if (!(delayed instanceof EventStoreMapKey)) {
            throw new UnsupportedOperationException();
        }
        final EventStoreMapKey that = (EventStoreMapKey) delayed;

        return Long.valueOf(this.expiredTimeMillis).compareTo(that.expiredTimeMillis);
    }

    @Override
    public String toString() {
        return "EventStoreMapKey [statisticType=" + statisticType + ", id=" + id + ", timeInterval=" + timeInterval
                + ", expiredTimeMillis=" + expiredTimeMillis + "]";
    }
}
