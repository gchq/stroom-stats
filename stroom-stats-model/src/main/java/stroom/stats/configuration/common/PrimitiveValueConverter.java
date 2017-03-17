

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

package stroom.stats.configuration.common;

import java.util.HashMap;
import java.util.Map;

public class PrimitiveValueConverter<E extends HasPrimitiveValue> {
    private Map<Byte, E> map;

    public PrimitiveValueConverter(E[] values) {
        map = new HashMap<>(values.length);
        for (E value : values) {
            map.put(value.getPrimitiveValue(), value);
        }
    }

    public E fromPrimitiveValue(byte i) {
        return map.get(i);
    }

    public void put(final Byte key, final E value) {
        map.put(key, value);
    }
}
