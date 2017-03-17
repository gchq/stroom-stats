

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

package stroom.stats.common;

import stroom.stats.api.StatisticTag;
import stroom.stats.common.rollup.RollUpBitMask;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class RollUpBitMaskUtil {
    /**
     * Builds a {@link RollUpBitMask} object from a list of StatisticTags where
     * the tag's value is either a value or '*' (to mark a roll up). The passed
     * list MUST be ordered by tag name as the positions of the tags in the list
     * directly relate to the bit mask positions.
     *
     * @param tags
     *            A list of {@link StatisticTag} objects ordered by their tag
     *            name
     * @return A new {@link RollUpBitMask} object
     */
    public static RollUpBitMask fromSortedTagList(final List<StatisticTag> tags) {
        final SortedSet<Integer> tagPositions = new TreeSet<>();
        int pos = 0;
        if (tags != null) {
            for (final StatisticTag tag : tags) {
                if (RollUpBitMask.ROLL_UP_TAG_VALUE.equals(tag.getValue())) {
                    tagPositions.add(pos);
                }
                pos++;
            }
        }
        return RollUpBitMask.fromTagPositions(tagPositions);
    }

}
