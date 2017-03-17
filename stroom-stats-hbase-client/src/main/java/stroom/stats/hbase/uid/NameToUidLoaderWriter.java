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

import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import stroom.stats.cache.AbstractReadOnlyCacheLoaderWriter;

import java.util.Map;
import java.util.Optional;

class NameToUidLoaderWriter extends AbstractReadOnlyCacheLoaderWriter<String, UID> {
    private final UniqueId uniqueId;

    public NameToUidLoaderWriter(final UniqueId uniqueId) {
        this.uniqueId = uniqueId;
    }

    @Override
    public UID load(final String name) throws Exception {
        return UID.from(Optional.ofNullable(uniqueId.getId(name))
                .orElseThrow(() -> new Exception(String.format("Name %s does not exist it uid table", name)))
                .get());
    }

    @Override
    public Map<String, UID> loadAll(final Iterable<? extends String> keys) throws BulkCacheLoadingException, Exception {
        throw new UnsupportedOperationException("LoadAll is not currently supported on this cache");
    }

}
