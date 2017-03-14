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

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestMultiPartIdentifier {

    @Test
    public void getValue() throws Exception {
        long idPart1 = 1001L;
        long idPart2 = 2002L;
        MultiPartIdentifier id = new MultiPartIdentifier(idPart1, idPart2);

        Assertions.assertThat(id.getValue()).containsExactly(idPart1, idPart2);
    }

    @Test
    public void getValue2() throws Exception {
        long idPart1 = 1001L;
        long idPart2 = 2002L;
        MultiPartIdentifier id = new MultiPartIdentifier(new Object[] {idPart1, idPart2});

        Assertions.assertThat(id.getValue()).containsExactly(idPart1, idPart2);
    }

}