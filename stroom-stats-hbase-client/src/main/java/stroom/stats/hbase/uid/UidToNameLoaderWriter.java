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
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;

class UidToNameLoaderWriter implements CacheLoaderWriter<UID, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UidToNameLoaderWriter.class);

    private final UniqueIdGenerator uniqueIdGenerator;

    @Inject
    public UidToNameLoaderWriter(final UniqueIdGenerator uniqueIdGenerator) {
        this.uniqueIdGenerator = uniqueIdGenerator;
    }

    @Override
    public String load(final UID uid) throws Exception {
        return Optional
                .ofNullable(uniqueIdGenerator.getName(uid.getUidBytes()))
                .orElseThrow(() -> new Exception(String.format("UID %s does not exist in the uid table", uid.toAllForms())))
                .get();
    }

    @Override
    public Map<UID, String> loadAll(final Iterable<? extends UID> keys) throws BulkCacheLoadingException, Exception {
        throw new UnsupportedOperationException("LoadAll is not currently supported on this cache");
    }

    @Override
    public void write(final UID key, final String value) throws Exception {
        LOGGER.trace("write called on key {} and value {}", key, value);
        //do nothing as the key/value will already have been written to the tables by this point by UniqueIdCacheImpl
    }

    @Override
    public void writeAll(final Iterable<? extends Map.Entry<? extends UID, ? extends String>> entries) throws BulkCacheWritingException, Exception {
        LOGGER.trace("writeAll called");
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }

    @Override
    public void delete(final UID key) throws Exception {
        LOGGER.trace("delete called on key {}", key);
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }

    @Override
    public void deleteAll(final Iterable<? extends UID> keys) throws BulkCacheWritingException, Exception {
        LOGGER.trace("deleteAll called");
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }
}
