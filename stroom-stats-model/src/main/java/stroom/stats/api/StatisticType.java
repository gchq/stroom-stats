

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
