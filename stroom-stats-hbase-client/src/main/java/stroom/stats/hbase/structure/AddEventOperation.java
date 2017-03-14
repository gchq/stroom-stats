

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

package stroom.stats.hbase.structure;

import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

public class AddEventOperation {
    private final EventStoreTimeIntervalEnum timeInterval;
    private final CellQualifier cellQualifier;
    private final RolledUpStatisticEvent rolledUpStatisticEvent;

    public AddEventOperation(final EventStoreTimeIntervalEnum timeInterval, final CellQualifier cellQualifier,
            final RolledUpStatisticEvent rolledUpStatisticEvent) {
        this.timeInterval = timeInterval;
        this.cellQualifier = cellQualifier;
        this.rolledUpStatisticEvent = rolledUpStatisticEvent;
    }

    public EventStoreTimeIntervalEnum getTimeInterval() {
        return timeInterval;
    }

    public CellQualifier getCellQualifier() {
        return cellQualifier;
    }

    public RolledUpStatisticEvent getRolledUpStatisticEvent() {
        return rolledUpStatisticEvent;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cellQualifier == null) ? 0 : cellQualifier.hashCode());
        result = prime * result + ((rolledUpStatisticEvent == null) ? 0 : rolledUpStatisticEvent.hashCode());
        result = prime * result + ((timeInterval == null) ? 0 : timeInterval.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final AddEventOperation other = (AddEventOperation) obj;
        if (cellQualifier == null) {
            if (other.cellQualifier != null)
                return false;
        } else if (!cellQualifier.equals(other.cellQualifier))
            return false;
        if (rolledUpStatisticEvent == null) {
            if (other.rolledUpStatisticEvent != null)
                return false;
        } else if (!rolledUpStatisticEvent.equals(other.rolledUpStatisticEvent))
            return false;
        if (timeInterval != other.timeInterval)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "AddEventOperation [timeInterval=" + timeInterval + ", cellQualifier=" + cellQualifier
                + ", rolledUpStatisticEvent=" + rolledUpStatisticEvent + "]";
    }
}
