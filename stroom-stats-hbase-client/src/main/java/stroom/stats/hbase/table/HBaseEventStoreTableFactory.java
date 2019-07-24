

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

package stroom.stats.hbase.table;

import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.structure.StatisticDataPointAdapterFactory;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.EnumMap;
import java.util.Map;

@Singleton
public class HBaseEventStoreTableFactory implements EventStoreTableFactory {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseEventStoreTableFactory.class);

    private final Map<EventStoreTimeIntervalEnum, HBaseEventStoreTable> eventStoreTables;
    private final StroomPropertyService propertyService;

    private final HBaseConnection hBaseConnection;

    @Inject
    public HBaseEventStoreTableFactory(final StroomPropertyService propertyService,
                                       final HBaseConnection hBaseConnection,
                                       final UniqueIdCache uniqueIdCache,
                                       final StatisticDataPointAdapterFactory statisticDataPointAdapterFactory) {

        LOGGER.debug(() -> String.format("Initialising: %s", this.getClass().getCanonicalName()));

        this.propertyService = propertyService;
        this.hBaseConnection = hBaseConnection;

        eventStoreTables = new EnumMap<>(EventStoreTimeIntervalEnum.class);

        // set up an event store table object for each time interval that we
        // use
        for (final EventStoreTimeIntervalEnum timeIntervalEnum : EventStoreTimeIntervalEnum.values()) {
            addEventStoreTable(timeIntervalEnum, uniqueIdCache, statisticDataPointAdapterFactory);
        }
    }

    @Override
    public EventStoreTable getEventStoreTable(final EventStoreTimeIntervalEnum timeInterval) {
        return eventStoreTables.get(timeInterval);
    }

    private void addEventStoreTable(final EventStoreTimeIntervalEnum timeInterval,
                                    final UniqueIdCache uniqueIdCache,
                                    final StatisticDataPointAdapterFactory statisticDataPointAdapterFactory) {

        eventStoreTables.put(timeInterval, new HBaseEventStoreTable(
                timeInterval,
                propertyService,
                hBaseConnection,
                uniqueIdCache,
                statisticDataPointAdapterFactory));
    }

    //TODO Implement alternative scheduling
    public void shutdown() {
    }

}
