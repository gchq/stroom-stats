

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

import stroom.stats.hbase.connection.HBaseConnection;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.task.api.TaskManager;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class HBaseTableFactory implements TableFactory {
    private final Map<EventStoreTimeIntervalEnum, HBaseEventStoreTable> eventStoreTables;
    private final HBaseUniqueIdForwardMapTable uniqueIdForwardMapTable;
    private final HBaseUniqueIdReverseMapTable uniqueIdReverseMapTable;

    private final TaskManager taskManager;
    private final StroomPropertyService propertyService;
    private final HBaseConnection hBaseConnection;

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(HBaseTableFactory.class);

    // A list of functions (provided by tables this factory produces) to be
    // called when Stroom shuts down. This is needed as the tables are not spring
    // beans and therefore can't have the shutdown hook
    private final List<Consumer<HBaseConnection>> shutdownFunctions = new ArrayList<>();

    private final List<HBaseTable> allTables = new ArrayList<>();

    @Inject
    public HBaseTableFactory(final TaskManager taskManager, final StroomPropertyService propertyService,
                             final HBaseConnection hBaseConnection, final UniqueIdCache uniqueIdCache) {
        LOGGER.info(() -> String.format("Initialising: %s", this.getClass().getCanonicalName()));

        this.taskManager = taskManager;
        this.propertyService = propertyService;
        this.hBaseConnection = hBaseConnection;

        eventStoreTables = new EnumMap<>(
                EventStoreTimeIntervalEnum.class);

        // set up an event store table object for each time interval that we
        // use
        for (final EventStoreTimeIntervalEnum timeIntervalEnum : EventStoreTimeIntervalEnum.values()) {
            addEventStoreTable(timeIntervalEnum, uniqueIdCache);
        }

        allTables.addAll(eventStoreTables.values());

        uniqueIdForwardMapTable = HBaseUniqueIdForwardMapTable.getInstance(hBaseConnection);
        allTables.add(uniqueIdForwardMapTable);

        uniqueIdReverseMapTable = HBaseUniqueIdReverseMapTable.getInstance(hBaseConnection);
        allTables.add(uniqueIdReverseMapTable);

    }

    @Override
    public EventStoreTable getEventStoreTable(final EventStoreTimeIntervalEnum timeInterval) {
        return (EventStoreTable) eventStoreTables.get(timeInterval);
    }

    @Override
    public UniqueIdForwardMapTable getUniqueIdForwardMapTable() {
        return (UniqueIdForwardMapTable) uniqueIdForwardMapTable;
    }

    @Override
    public UniqueIdReverseMapTable getUniqueIdReverseMapTable() {
        return (UniqueIdReverseMapTable) uniqueIdReverseMapTable;
    }

    @Override
    public List<GenericTable> getAllTables() {
        return new ArrayList<>(allTables);
    }

    private void addEventStoreTable(final EventStoreTimeIntervalEnum timeInterval, final UniqueIdCache uniqueIdCache) {
        eventStoreTables.put(timeInterval,
                HBaseEventStoreTable.getInstance(timeInterval, taskManager, propertyService, hBaseConnection, uniqueIdCache));
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
