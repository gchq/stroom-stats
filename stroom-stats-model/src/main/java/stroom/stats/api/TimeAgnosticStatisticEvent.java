

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

package stroom.stats.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//TODO should use generics for the count/value stuff
public class TimeAgnosticStatisticEvent implements Serializable {
    private static final long serialVersionUID = 6308709037286356273L;
    private final String name;
    //TODO should probably be a set as we don't care if the tags arrive out of order
    //Could stay as a list if the hashing was order agnostic
    private final List<StatisticTag> tagList;
    private final Long count;
    private final Double value;
    private final StatisticType statisticType;
    // hold a cache of the positions of the tag names in the tagList so we can
    // quickly get them

    private final Map<String, Integer> tagPositionMap;
    private final int hashCode;

    /**
     * @param name
     * @param tagList
     *            Must be ordered by tag name. Can be null.
     * @param count
     */
    public TimeAgnosticStatisticEvent(final String name, final List<StatisticTag> tagList, final Long count) {
        this.name = name;
        this.tagList = buildTagList(tagList);
        this.tagPositionMap = buildTagPositionMap();
        this.value = null;
        this.count = count;
        statisticType = StatisticType.COUNT;
        hashCode = buildHashCode();

        if (count == null) {
            throw new IllegalArgumentException("Statistic must have a count or value");
        }
    }

    /**
     * @param name
     * @param tagList
     *            Must be ordered by tag name. Can be null.
     * @param value
     */
    public TimeAgnosticStatisticEvent(final String name, final List<StatisticTag> tagList, final Double value) {
        this.name = name;
        this.tagList = buildTagList(tagList);
        this.tagPositionMap = buildTagPositionMap();
        this.value = value;
        this.count = null;
        statisticType = StatisticType.VALUE;
        hashCode = buildHashCode();

        if (value == null && count == null) {
            throw new IllegalArgumentException("Statistic must have a count or value");
        }
    }

    private List<StatisticTag> buildTagList(final List<StatisticTag> tagList) {
        // build a new list object, sort it and make it unmodifiable
        // or just return an unmodifiable empty list
        if (tagList != null) {
            final List<StatisticTag> tempTagList = new ArrayList<>(tagList);

            return Collections.unmodifiableList(tempTagList);
        } else {
            return Collections.unmodifiableList(Collections.<StatisticTag> emptyList());
        }
    }

    private Map<String, Integer> buildTagPositionMap() {
        final Map<String, Integer> tagPositionMap = new HashMap<>();
        if (tagList != null) {
            int i = 0;
            for (final StatisticTag tag : tagList) {
                tagPositionMap.put(tag.getTag(), i++);
                ;
            }
        }
        return tagPositionMap;
    }

    public List<StatisticTag> getTagList() {
        return tagList;
    }

    public StatisticType getType() {
        if (value != null) {
            return StatisticType.VALUE;
        } else {
            return StatisticType.COUNT;
        }
    }

    public String getName() {
        return name;
    }

    public Double getValue() {
        return value;
    }

    public Long getCount() {
        return count;
    }

    public StatisticType getStatisticType() {
        return statisticType;
    }

    /**
     * @param tagName
     *            The name of the tag in a {@link StatisticTag} object
     * @return The position of the tag in the tag list (0 based)
     */
    public Integer getTagPosition(final String tagName) {
        return tagPositionMap.get(tagName);
    }

    @Override
    public String toString() {
        return "TimeAgnosticStatisticEvent [name=" + name + ", tagList=" + tagList + ", count=" + count + ", value="
                + value + ", statisticType=" + statisticType + "]";
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((count == null) ? 0 : count.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((statisticType == null) ? 0 : statisticType.hashCode());
        result = prime * result + ((tagList == null) ? 0 : tagList.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TimeAgnosticStatisticEvent other = (TimeAgnosticStatisticEvent) obj;
        if (count == null) {
            if (other.count != null)
                return false;
        } else if (!count.equals(other.count))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (statisticType != other.statisticType)
            return false;
        if (tagList == null) {
            if (other.tagList != null)
                return false;
        } else if (!tagList.equals(other.tagList))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

}
