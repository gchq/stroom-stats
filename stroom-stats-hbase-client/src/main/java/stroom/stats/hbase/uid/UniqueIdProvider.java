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

package stroom.stats.hbase.uid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.table.UniqueIdForwardMapTable;
import stroom.stats.hbase.table.UniqueIdReverseMapTable;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class UniqueIdProvider implements Provider<UniqueId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueIdProvider.class);

    private final UniqueId uniqueId;

    @Inject
    public UniqueIdProvider(final UniqueIdForwardMapTable uniqueIdForwardMapTable,
                            final UniqueIdReverseMapTable uniqueIdReverseMapTable) {

        this.uniqueId = new UniqueId(uniqueIdForwardMapTable, uniqueIdReverseMapTable, UID.UID_ARRAY_LENGTH);
    }


    /**
     * Provides a fully-constructed and injected instance of {@code T}.
     *
     * @throws RuntimeException if the injector encounters an error while
     *                          providing an instance. For example, if an injectable member on
     *                          {@code T} throws an exception, the injector may wrap the exception
     *                          and throw it to the caller of {@code get()}. Callers should not try
     *                          to handle such exceptions as the behavior may vary across injector
     *                          implementations and even different configurations of the same injector.
     */
    @Override
    public UniqueId get() {
        return uniqueId;
    }
}
