

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

import stroom.stats.configuration.common.HasDisplayValue;
import stroom.stats.configuration.common.HasPrimitiveValue;
import stroom.stats.configuration.common.PrimitiveValueConverter;

import java.io.Serializable;

/**
 * Used to distinguish the type of a statistic event. COUNT type events can be
 * summed, e.g. the number of bytes written however VALUE events cannot, e.g.
 * cpu%.
 */
public enum StatisticType implements HasDisplayValue,HasPrimitiveValue,Serializable {
    COUNT("Count", 1), VALUE("Value", 2);

    private final String displayValue;
    private final byte primitiveValue;

    public static final PrimitiveValueConverter<StatisticType> PRIMITIVE_VALUE_CONVERTER = new PrimitiveValueConverter<>(
            StatisticType.values());

    private StatisticType(final String displayValue, final int primitiveValue) {
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
