

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

package stroom.stats.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.TimeAgnosticStatisticEvent;
import stroom.stats.hbase.structure.TimeAgnosticRowKey;
import stroom.stats.hbase.uid.UniqueIdCache;

public interface RowKeyCache {
    Logger LOGGER = LoggerFactory.getLogger(RowKeyCache.class);

    TimeAgnosticRowKey getTimeAgnosticRowKey(TimeAgnosticStatisticEvent timeAgnosticStatisticEvent);

    static TimeAgnosticRowKey getTimeAgnosticRowKeyUncached(final TimeAgnosticStatisticEvent timeAgnosticStatisticEvent,
                                                            final UniqueIdCache uniqueIdCache) {
        final TimeAgnosticRowKey agnosticRowKey = (new SimpleRowKeyBuilder(uniqueIdCache, null))
                .buildTimeAgnosticRowKey(timeAgnosticStatisticEvent);

        LOGGER.trace("Adding key [{}] and value [{}] to the row key cache", timeAgnosticStatisticEvent, agnosticRowKey);

        return agnosticRowKey;
    }
}
