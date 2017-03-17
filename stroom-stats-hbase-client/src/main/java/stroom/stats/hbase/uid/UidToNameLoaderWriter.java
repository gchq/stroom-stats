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

class UidToNameLoaderWriter extends AbstractReadOnlyCacheLoaderWriter<UID, String> {
    private final UniqueId uniqueId;

    public UidToNameLoaderWriter(final UniqueId uniqueId) {
        this.uniqueId = uniqueId;
    }

    @Override
    public String load(final UID uid) throws Exception {
        return Optional.ofNullable(uniqueId.getName(uid.getUidBytes()))
                .orElseThrow(() -> new Exception(String.format("UID %s does not exist in the uid table", uid.toAllForms())))
                .get();
    }

    @Override
    public Map<UID, String> loadAll(final Iterable<? extends UID> keys) throws BulkCacheLoadingException, Exception {
        throw new UnsupportedOperationException("LoadAll is not currently supported on this cache");
    }
}
