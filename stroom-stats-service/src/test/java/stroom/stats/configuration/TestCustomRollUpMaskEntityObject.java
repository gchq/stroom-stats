

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
