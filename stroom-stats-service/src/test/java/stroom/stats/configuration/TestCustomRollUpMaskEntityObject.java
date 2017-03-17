

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

package stroom.stats.configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestCustomRollUpMaskEntityObject {
    @Test
    public void testIsTagRolledUp() throws Exception {
        final CustomRollUpMask mask = new CustomRollUpMaskEntityObject(Arrays.asList(3, 1, 0));

        Assert.assertTrue(mask.isTagRolledUp(3));
        Assert.assertFalse(mask.isTagRolledUp(2));
        Assert.assertTrue(mask.isTagRolledUp(1));
        Assert.assertTrue(mask.isTagRolledUp(0));
    }

    @Test
    public void testSetRollUpState() throws Exception {
        final CustomRollUpMask mask = new CustomRollUpMaskEntityObject(Arrays.asList(3, 1, 0));

        Assert.assertFalse(mask.isTagRolledUp(2));
        Assert.assertTrue(mask.isTagRolledUp(3));

        ((CustomRollUpMaskEntityObject)mask).setRollUpState(2, false);
        Assert.assertFalse(mask.isTagRolledUp(2));

        ((CustomRollUpMaskEntityObject)mask).setRollUpState(2, true);
        Assert.assertTrue(mask.isTagRolledUp(2));

        ((CustomRollUpMaskEntityObject)mask).setRollUpState(2, false);
        Assert.assertFalse(mask.isTagRolledUp(2));

        ((CustomRollUpMaskEntityObject)mask).setRollUpState(3, true);
        Assert.assertTrue(mask.isTagRolledUp(3));

        ((CustomRollUpMaskEntityObject)mask).setRollUpState(3, false);
        Assert.assertFalse(mask.isTagRolledUp(3));
    }
}
