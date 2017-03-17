

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

package stroom.stats.api;


import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Class to hold a statistic event, ie. the count or value of something in a
 * given time window
 */
public class StatisticEvent {
    private final long timeMs;
    private final List<MultiPartIdentifier> identifiers;
    private final TimeAgnosticStatisticEvent timeAgnosticStatisticEvent;

    //TODO add in a list of compound event identifiers to link back to the events
    //that contributed to this stat

    /**
     * Constructor for value type events with floating point values
     *
     * @param timeMs
     *            time of the event in millis since epoch
     * @param name
     *            the name of the event
     * @param tagList
     *            list of tag/value pairs that describe the event. Must be
     *            ordered by tag name. Can be null.
     * @param count
     *            the count of the event, e.g. the number of desktop logons
     */
    public StatisticEvent(final long timeMs, final String name, final List<StatisticTag> tagList,
                           final long count) {
        this(timeMs, name, tagList, Collections.emptyList(), count);
    }
    /**
     * Constructor for value type events with floating point values
     *
     * @param timeMs
     *            time of the event in millis since epoch
     * @param name
     *            the name of the event
     * @param tagList
     *            list of tag/value pairs that describe the event. Must be
     *            ordered by tag name. Can be null.
     * @param count
     *            the count of the event, e.g. the number of desktop logons
     */
    public StatisticEvent(final long timeMs, final String name, final List<StatisticTag> tagList,
                          final List<MultiPartIdentifier> identifiers, final long count) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(identifiers);
        this.timeMs = timeMs;
        this.identifiers = identifiers;

        this.timeAgnosticStatisticEvent = new TimeAgnosticStatisticEvent(name, tagList, count);

    }

    /**
     * Constructor for value type events with floating point values
     *
     * @param timeMs
     *            time of the event in millis since epoch
     * @param name
     *            the name of the event
     * @param tagList
     *            list of tag/value pairs that describe the event. Must be
     *            ordered by tag name. Can be null.
     * @param value
     *            the value of the event, e.g. the heap size in MB, bytes read,
     *            etc.
     */
    public StatisticEvent(final long timeMs, final String name, final List<StatisticTag> tagList,
                          final double value) {
        this(timeMs, name, tagList, Collections.emptyList(), value);
    }
    /**
     * Constructor for value type events with floating point values
     *
     * @param timeMs
     *            time of the event in millis since epoch
     * @param name
     *            the name of the event
     * @param tagList
     *            list of tag/value pairs that describe the event. Must be
     *            ordered by tag name. Can be null.
     * @param value
     *            the value of the event, e.g. the heap size in MB, bytes read,
     *            etc.
     */
    public StatisticEvent(final long timeMs, final String name, final List<StatisticTag> tagList,
                          final List<MultiPartIdentifier> identifiers, final double value) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(identifiers);
        this.timeMs = timeMs;
        this.identifiers = identifiers;
        this.timeAgnosticStatisticEvent = new TimeAgnosticStatisticEvent(name, tagList, value);
    }

    /**
     * @return time in milliseconds since epoch
     */
    public long getTimeMs() {
        return timeMs;
    }

    public List<StatisticTag> getTagList() {
        return timeAgnosticStatisticEvent.getTagList();
    }

    public StatisticType getType() {
        return timeAgnosticStatisticEvent.getStatisticType();
    }

    public String getName() {
        return timeAgnosticStatisticEvent.getName();
    }

    public Double getValue() {
        return timeAgnosticStatisticEvent.getValue();
    }

    public Long getCount() {
        return timeAgnosticStatisticEvent.getCount();
    }

    public TimeAgnosticStatisticEvent getTimeAgnosticStatisticEvent() {
        return timeAgnosticStatisticEvent;
    }

    /**
     * @param tagName
     *            The name of the tag in a {@link StatisticTag} object
     * @return The position of the tag in the tag list (0 based)
     */
    public Integer getTagPosition(final String tagName) {
        return timeAgnosticStatisticEvent.getTagPosition(tagName);
    }

    @Override
    public String toString() {
        return "StatisticEvent{" +
                "timeMs=" + timeMs +
                ", identifiers=" + identifiers +
                ", timeAgnosticStatisticEvent=" + timeAgnosticStatisticEvent +
                '}';
    }

    public StatisticEvent duplicateWithNewTagList(final List<StatisticTag> newTagList) {
        if (timeAgnosticStatisticEvent.getStatisticType().equals(StatisticType.COUNT)) {
            return new StatisticEvent(timeMs, timeAgnosticStatisticEvent.getName(), newTagList, identifiers,
                    timeAgnosticStatisticEvent.getCount());
        } else {
            return new StatisticEvent(timeMs, timeAgnosticStatisticEvent.getName(), newTagList, identifiers,
                    timeAgnosticStatisticEvent.getValue());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatisticEvent that = (StatisticEvent) o;

        if (timeMs != that.timeMs) return false;
        if (!identifiers.equals(that.identifiers)) return false;
        return timeAgnosticStatisticEvent.equals(that.timeAgnosticStatisticEvent);
    }

    @Override
    public int hashCode() {
        int result = (int) (timeMs ^ (timeMs >>> 32));
        result = 31 * result + identifiers.hashCode();
        result = 31 * result + timeAgnosticStatisticEvent.hashCode();
        return result;
    }
}
