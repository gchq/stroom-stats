

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

package stroom.stats.util;

import org.junit.Assert;
import org.junit.Test;

public class TestModelStringUtil {
    @Test
    public void testTimeSizeDividerNull() {
        doTest("", null);
    }

    @Test
    public void testTimeSizeDivider1() {
        doTest("1", 1L);
    }

    @Test
    public void testTimeSizeDivider1000() {
        doTest("1000", 1000L);
    }

    @Test
    public void testTimeSizeDivider1Ms() {
        doTest("1MS", 1L);
    }

    @Test
    public void testTimeSizeDivider1ms() {
        doTest("1 ms", 1L);
    }

    @Test
    public void testTimeSizeDivider1s() {
        doTest("1 s", 1000L);
    }

    @Test
    public void testTimeSizeDivider1m() {
        doTest("1 m", 60 * 1000L);
    }

    @Test
    public void testTimeSizeDivider1h() {
        doTest("1 h", 60 * 60 * 1000L);
    }

    @Test
    public void testTimeSizeDivider1d() {
        doTest("1 d", 24 * 60 * 60 * 1000L);
    }

    private Long doTest(String input, Long expected) {
        Long output = ModelStringUtil.parseDurationString(input);

        Assert.assertEquals(expected, output);

        System.out.println(input + " = " + output);

        return output;

    }

}
