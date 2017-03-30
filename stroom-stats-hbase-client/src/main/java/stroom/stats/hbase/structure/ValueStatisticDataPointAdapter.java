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

package stroom.stats.hbase.structure;

import stroom.stats.api.StatisticTag;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.ValueStatisticDataPoint;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.List;

public class ValueStatisticDataPointAdapter implements StatisticDataPointAdapter {

    @Override
    public StatisticDataPoint convertCell(final long timeMs,
                                          final EventStoreTimeIntervalEnum interval,
                                          final List<StatisticTag> tags,
                                          final byte[] bytes,
                                          final int cellValueOffset,
                                          final int cellValueLength) {

        final ValueCellValue cellValue = new ValueCellValue(bytes, cellValueOffset, cellValueLength);

        return new ValueStatisticDataPoint(timeMs,
                interval.columnInterval(),
                tags,
                cellValue.getCount(),
                cellValue.getAverageValue(),
                cellValue.getMinValue(),
                cellValue.getMaxValue());
    }
}
