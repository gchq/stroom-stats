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