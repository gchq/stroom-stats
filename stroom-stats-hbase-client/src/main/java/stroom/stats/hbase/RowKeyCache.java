

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
