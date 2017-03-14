

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
import java.util.Comparator;

public class StatisticTag implements Serializable, Comparable<StatisticTag> {
    private static final long serialVersionUID = 1647083366837339057L;

    public static final String NULL_VALUE_STRING = "<<<<NULL_VALUE>>>>";

    private final String tag;
    private final String value;

    private final int hashCode;

    public StatisticTag(final String tag, final String value) {
        if (tag == null || tag.length() == 0) {
            throw new RuntimeException("Cannot have a statistic tag with null or zero length name");
        }
        this.tag = tag;
        this.value = value;
        hashCode = buildHashCode();
    }

    public String getTag() {
        return tag;
    }

    public String getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tag == null) ? 0 : tag.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof StatisticTag))
            return false;
        final StatisticTag other = (StatisticTag) obj;
        if (tag == null) {
            if (other.getTag() != null)
                return false;
        } else if (!tag.equals(other.getTag()))
            return false;
        if (value == null) {
            if (other.getValue() != null)
                return false;
        } else if (!value.equals(other.getValue()))
            return false;
        return true;
    }

    @Override
    public int compareTo(final StatisticTag o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return "StatisticTag [tag=" + tag + ", value=" + value + "]";
    }

    public static class TagNameComparator implements Comparator<StatisticTag> {
        @Override
        public int compare(final StatisticTag tag1, final StatisticTag tag2) {
            if (tag1 == null || tag2 == null) {
                throw new RuntimeException("Tag list contains null elements, this should not happen");
            }
            // compare on just the tag name, not the value as tag name should be
            // unique in the list.
            // Tag name cannot be null.
            return tag1.getTag().compareTo(tag2.getTag());
        }
    }
}
