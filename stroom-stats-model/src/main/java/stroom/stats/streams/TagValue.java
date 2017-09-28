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
import stroom.stats.hbase.uid.UID;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class TagValue implements Comparable<TagValue> {
    static int TAG_PART_LENGTH = UID.UID_ARRAY_LENGTH;
    static int TAG_PART_OFFSET = 0;
    static int VALUE_PART_LENGTH = UID.UID_ARRAY_LENGTH;
    static int VALUE_PART_OFFSET = TAG_PART_OFFSET + TAG_PART_LENGTH;
    static int TAG_VALUE_PAIR_LENGTH = TAG_PART_LENGTH + VALUE_PART_LENGTH;

    public static final Comparator<TagValue> TAG_THEN_VALUE_COMPARATOR = Comparator
            .comparing(TagValue::getTag)
            .thenComparing(TagValue::getValue);

    public static final Comparator<List<TagValue>> TAG_VALUES_COMPARATOR = (o1, o2) -> {
        for (int i = 0; i < Math.min(o1.size(), o2.size()); i++) {
            int compareVal = o1.get(i).compareTo(o2.get(i));
            if (compareVal != 0) {
                return compareVal;
            }
        }
        return Integer.compare(o1.size(), o2.size());
    };

    private final UID tag;
    private final UID value;
    //cache the hashcode, though this relies on the underlying byte[]s not being mutated
    private final int hashCode;


    public TagValue(final byte[] bytes, int offset) {
        Preconditions.checkNotNull(bytes);
        Preconditions.checkNotNull(offset);

        this.tag = UID.from(bytes, offset);
        offset += TAG_PART_LENGTH;
        this.value = UID.from(bytes, offset);

        this.hashCode = buildHashCode();
    }

    public TagValue(final UID tag, final UID value) {
        Preconditions.checkNotNull(tag);
        Preconditions.checkNotNull(value);

        this.tag = tag;
        this.value = value;

        this.hashCode = buildHashCode();
    }

    /**
     * Shallow copy, replacing the value with a reference to the rolled up value
     *
     * @param rolledUpValue
     * @return
     */
    public TagValue cloneAndRollUp(final UID rolledUpValue) {
        Preconditions.checkNotNull(rolledUpValue);
        return new TagValue(this.tag, rolledUpValue);
    }

    public byte[] getBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(TAG_VALUE_PAIR_LENGTH);
        byteBuffer.put(tag.getUidBytes());
        byteBuffer.put(value.getUidBytes());
        return byteBuffer.array();
    }

    public UID getTag() {
        return tag;
    }

    public UID getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TagValue tagValue = (TagValue) o;

        if (!tag.equals(tagValue.tag)) return false;
        return value.equals(tagValue.value);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int buildHashCode() {
        int result = tag.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TagValue{" +
                "tag=" + tag +
                ", value=" + value +
                '}';
    }

    @Override
    public int compareTo(final TagValue other) {
        return TAG_THEN_VALUE_COMPARATOR.compare(this, other);
    }
}
