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

package stroom.stats.hbase.uid;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestUID {

    byte[] bytes = new byte[]{1, 2, 3, 4};
    byte[] bigBytes = new byte[]{9, 9, 1, 2, 3, 4, 8, 8};
    byte[] otherBytes = new byte[] {4,3,2,1};
    int bigBytesOffset = 2;

    @Test
    public void of() throws Exception {
        UID uid = UID.from(bytes);
        Assertions.assertThat(uid.getUidBytes()).isEqualTo(bytes);
        Assertions.assertThat(uid.getBackingArray()).isEqualTo(bytes);
    }

    @Test
    public void of_withOffset() throws Exception {
        UID uid = UID.from(bigBytes, bigBytesOffset);
        Assertions.assertThat(uid.getUidBytes()).isEqualTo(bytes);
        Assertions.assertThat(uid.getBackingArray()).isEqualTo(bigBytes);
    }

    @Test
    public void getBackingArray() throws Exception {
        //tested above
    }

    @Test
    public void getUidBytes() throws Exception {
        //tested above
    }

    @Test
    public void testHashCode() throws Exception {
        UID uid1 = UID.from(bytes);
        UID uid2 = UID.from(bigBytes, bigBytesOffset);

        Assertions.assertThat(uid1).isEqualTo(uid2);
        Assertions.assertThat(uid1.hashCode()).isEqualTo(uid2.hashCode());
    }

    @Test
    public void equals() throws Exception {
        UID uid1 = UID.from(bytes);
        UID uid2 = UID.from(otherBytes);

        Assertions.assertThat(uid1).isNotEqualTo(uid2);
    }

    @Test
    public void compareTo() throws Exception {
        UID uid1 = UID.from(bytes);
        UID uid2 = UID.from(otherBytes);
        UID uid3 = UID.from(bigBytes, bigBytesOffset);

        Assertions.assertThat(uid1.compareTo(uid2)).isLessThan(0);
        Assertions.assertThat(uid1.compareTo(uid3)).isEqualTo(0);
    }

    @Test
    public void compareTo_otherBytes() throws Exception {
        UID uid1 = UID.from(bytes);

        Assertions.assertThat(uid1.compareTo(bigBytes, bigBytesOffset)).isEqualTo(0);
        Assertions.assertThat(uid1.compareTo(otherBytes, 0)).isLessThan(0);
    }
}