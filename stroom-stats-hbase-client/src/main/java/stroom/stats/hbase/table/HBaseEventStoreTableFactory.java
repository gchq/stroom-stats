

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
import stroom.stats.task.api.TaskManager;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Singleton
public class HBaseEventStoreTableFactory implements EventStoreTableFactory {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseEventStoreTableFactory.class);

    private final Map<EventStoreTimeIntervalEnum, HBaseEventStoreTable> eventStoreTables;
    private final TaskManager taskManager;
    private final StroomPropertyService propertyService;

    private final HBaseConnection hBaseConnection;

    // A list of functions (provided by tables this factory produces) to be
    // called when Stroom shuts down. This is needed as the tables are not spring
    // beans and therefore can't have the shutdown hook
    private final List<Consumer<HBaseConnection>> shutdownFunctions = new ArrayList<>();

    @Inject
    public HBaseEventStoreTableFactory(final TaskManager taskManager,
                                       final StroomPropertyService propertyService,
                                       final HBaseConnection hBaseConnection,
                                       final UniqueIdCache uniqueIdCache,
                                        final StatisticDataPointAdapterFactory statisticDataPointAdapterFactory) {

        LOGGER.debug(() -> String.format("Initialising: %s", this.getClass().getCanonicalName()));

        this.taskManager = taskManager;
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

        eventStoreTables.put(timeInterval,
                HBaseEventStoreTable.getInstance(timeInterval,
                        propertyService,
                        hBaseConnection,
                        uniqueIdCache,
                        statisticDataPointAdapterFactory));
    }


    public void regsiterShutdownFunction(final Consumer<HBaseConnection> shutdownFunction) {
        this.shutdownFunctions.add(shutdownFunction);
    }

    //TODO Implement alternative scheduling
//    @StroomShutdown
    public void shutdown() {
        // Call each of the registered shutdown functions
        shutdownFunctions.forEach((shutdownFunc) -> shutdownFunc.accept(hBaseConnection));
    }

}
