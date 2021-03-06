

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


import stroom.stats.configuration.common.HasDisplayValue;
import stroom.stats.configuration.common.HasPrimitiveValue;
import stroom.stats.configuration.common.PrimitiveValueConverter;

import java.io.Serializable;

public enum StatisticRollUpType implements HasDisplayValue,HasPrimitiveValue,Serializable {
    NONE("None", 1), ALL("All", 2), CUSTOM("Custom", 3);

    private final String displayValue;
    private final byte primitiveValue;

    public static final PrimitiveValueConverter<StatisticRollUpType> PRIMITIVE_VALUE_CONVERTER = new PrimitiveValueConverter<>(
            StatisticRollUpType.values());

    private StatisticRollUpType(final String displayValue, final int primitiveValue) {
        this.displayValue = displayValue;
        this.primitiveValue = (byte) primitiveValue;
    }

    @Override
    public String getDisplayValue() {
        return displayValue;
    }

    @Override
    public byte getPrimitiveValue() {
        return primitiveValue;
    }
}
