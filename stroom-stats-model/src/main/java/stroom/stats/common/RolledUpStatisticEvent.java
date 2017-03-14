

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

package stroom.stats.common;


import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.TimeAgnosticStatisticEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Wrapper for a {@link StatisticEvent} that adds a list of {@link stroom.stats.api.StatisticTag}
 * lists, i.e. the different rollup permutations of the original statistic tag
 * list. The iterator allows you to get access to the underlying
 * {@link TimeAgnosticStatisticEvent} objects which are built on the fly.
 */
public class RolledUpStatisticEvent implements Iterable<TimeAgnosticStatisticEvent> {
    private final StatisticEvent originalStatisticEvent;
    private final List<List<StatisticTag>> tagListPermutations;

    public RolledUpStatisticEvent(StatisticEvent originalStatisticEvent, List<List<StatisticTag>> tagListPermutations) {
        this.originalStatisticEvent = originalStatisticEvent;
        this.tagListPermutations = tagListPermutations;
    }

    /**
     * To be used when no roll ups are required
     */
    public RolledUpStatisticEvent(StatisticEvent originalStatisticEvent) {
        this.originalStatisticEvent = originalStatisticEvent;
        this.tagListPermutations = new ArrayList<>();
        this.tagListPermutations.add(originalStatisticEvent.getTagList());
    }

    public long getTimeMs() {
        return originalStatisticEvent.getTimeMs();
    }

    // public long getPrecisionMs() {
    // return originalStatisticEvent.getPrecisionMs();
    // }

    public StatisticType getType() {
        return originalStatisticEvent.getType();
    }

    public String getName() {
        return originalStatisticEvent.getName();
    }

    public Double getValue() {
        return originalStatisticEvent.getValue();
    }

    public Long getCount() {
        return originalStatisticEvent.getCount();

    }
    public int getPermutationCount() {
        return tagListPermutations.size();
    }

    @Override
    public Iterator<TimeAgnosticStatisticEvent> iterator() {
        return new Iterator<TimeAgnosticStatisticEvent>() {
            int nextElement = 0;

            @Override
            public boolean hasNext() {
                if (tagListPermutations == null || tagListPermutations.size() == 0) {
                    return false;
                }

                return nextElement < tagListPermutations.size();
            }

            @Override
            public TimeAgnosticStatisticEvent next() {
                if (tagListPermutations == null || tagListPermutations.size() == 0) {
                    return null;
                }

                // the tag list embedded in the originalStatisticEvent is
                // ignored as it will be contained within the
                // tagListPermutations
                if (originalStatisticEvent.getType().equals(StatisticType.COUNT)) {
                    return new TimeAgnosticStatisticEvent(originalStatisticEvent.getName(),
                            tagListPermutations.get(nextElement++), originalStatisticEvent.getCount());
                } else {
                    return new TimeAgnosticStatisticEvent(originalStatisticEvent.getName(),
                            tagListPermutations.get(nextElement++), originalStatisticEvent.getValue());
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove is not supported for this class");

            }
        };
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((originalStatisticEvent == null) ? 0 : originalStatisticEvent.hashCode());
        result = prime * result + ((tagListPermutations == null) ? 0 : tagListPermutations.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RolledUpStatisticEvent other = (RolledUpStatisticEvent) obj;
        if (originalStatisticEvent == null) {
            if (other.originalStatisticEvent != null)
                return false;
        } else if (!originalStatisticEvent.equals(other.originalStatisticEvent))
            return false;
        if (tagListPermutations == null) {
            if (other.tagListPermutations != null)
                return false;
        } else if (!tagListPermutations.equals(other.tagListPermutations))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RolledUpStatisticEvent [originalStatisticEvent=" + originalStatisticEvent + ", tagListPermutations="
                + tagListPermutations + "]";
    }

}
