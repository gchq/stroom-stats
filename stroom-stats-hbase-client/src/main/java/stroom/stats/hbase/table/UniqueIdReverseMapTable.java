

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

import stroom.stats.hbase.uid.UID;

import java.util.Optional;

/**
 * Table that maps a UID byte array to a String
 */
public interface UniqueIdReverseMapTable extends GenericTable {

    /**
     * @return True if the put succeeded, false if the name already existed
     */
    boolean putNameIfNotExists(final byte[] newUid, final byte[] name);

    long nextId();

    Optional<String> getNameAsString(final UID uid);

    boolean checkAndDeleteName(final byte[] newUid, final byte[] name);
}
