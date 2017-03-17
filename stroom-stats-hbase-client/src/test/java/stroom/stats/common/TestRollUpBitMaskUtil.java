

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

import org.junit.Assert;
import org.junit.Test;
import stroom.stats.api.StatisticTag;
import stroom.stats.common.rollup.RollUpBitMask;

import java.util.ArrayList;
import java.util.List;

public class TestRollUpBitMaskUtil {
    @Test
    public void testFromSortedTagList() {
        final List<StatisticTag> tagList = new ArrayList<StatisticTag>();
        tagList.add(new StatisticTag("Tag0", "someValue"));
        tagList.add(new StatisticTag("Tag1", RollUpBitMask.ROLL_UP_TAG_VALUE));
        tagList.add(new StatisticTag("Tag2", "someValue"));
        tagList.add(new StatisticTag("Tag3", RollUpBitMask.ROLL_UP_TAG_VALUE));

        final RollUpBitMask rowKeyBitMap = RollUpBitMaskUtil.fromSortedTagList(tagList);

        Assert.assertEquals("000000000001010", rowKeyBitMap.toString());
    }

    @Test
    public void testFromSortedTagListEmptyList() {
        final List<StatisticTag> tagList = new ArrayList<StatisticTag>();

        final RollUpBitMask rowKeyBitMap = RollUpBitMaskUtil.fromSortedTagList(tagList);

        Assert.assertEquals("000000000000000", rowKeyBitMap.toString());
    }

    @Test
    public void testFromSortedTagListNullList() {
        final RollUpBitMask rowKeyBitMap = RollUpBitMaskUtil.fromSortedTagList(null);

        Assert.assertEquals("000000000000000", rowKeyBitMap.toString());
    }
}
