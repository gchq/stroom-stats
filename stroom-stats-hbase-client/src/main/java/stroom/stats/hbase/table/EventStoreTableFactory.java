

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

package stroom.stats.hbase.table;

import stroom.stats.shared.EventStoreTimeIntervalEnum;


public interface EventStoreTableFactory {
    /**
     * Returns a cached instance of an EventStoreTable with an implementation
     * dependent on the EventStoreTableFactory instance used.
     *
     * @param timeinterval
     *            The time interval to create or get the EventStoreTable for
     * @return A cached EventStoreTable instance
     */
    EventStoreTable getEventStoreTable(EventStoreTimeIntervalEnum timeinterval);

}
