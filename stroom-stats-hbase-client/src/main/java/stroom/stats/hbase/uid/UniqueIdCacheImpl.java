

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

package stroom.stats.hbase.uid;

import com.google.common.base.Preconditions;
import javaslang.control.Try;
import org.ehcache.Cache;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.cache.AbstractReadOnlyCacheLoaderWriter;
import stroom.stats.cache.CacheFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;

public class UniqueIdCacheImpl implements UniqueIdCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueIdCacheImpl.class);

    static final String NAME_TO_UID_CACHE_NAME = "nameToUidCache";
    static final String UID_TO_NAME_CACHE_NAME = "uidToNameCache";

    private final UniqueId uniqueId;

    private final Cache<String, UID> nameToUidCache;
    private final Cache<UID, String> uidToNameCache;

    @Inject
    public UniqueIdCacheImpl(final UniqueId uniqueId, final CacheFactory cacheFactory) {
        this.uniqueId = uniqueId;

        NameToUidLoaderWriter nameToUidLoaderWriter = new NameToUidLoaderWriter(uniqueId);
        UidToNameLoaderWriter uidToNameLoaderWriter = new UidToNameLoaderWriter(uniqueId);

        this.nameToUidCache = cacheFactory.getOrCreateCache(NAME_TO_UID_CACHE_NAME, String.class, UID.class, Optional.of(nameToUidLoaderWriter));
        this.uidToNameCache = cacheFactory.getOrCreateCache(UID_TO_NAME_CACHE_NAME, UID.class, String.class, Optional.of(uidToNameLoaderWriter));
    }

    @Override
    public UID getOrCreateId(final String name) {
        Preconditions.checkNotNull(name, "A null name is not valid");

        return Try.of(() -> nameToUidCache.get(name))
                .getOrElse(() -> {
                    //not in cache or table so create it
                    //in the event that another thread does this as well then getOrCreateId will handle
                    //thread safety issues
                    UID newUid = UID.from(uniqueId.getOrCreateId(name));
                    //add the new K/V to the cache as chances are somebody else will need them
                    nameToUidCache.put(name, newUid);
                    return newUid;
                });
    }

    @Override
    public Try<UID> getUniqueId(final String name) {
        Preconditions.checkNotNull(name, "A null name is not valid");
        return Try.of(() -> nameToUidCache.get(name));
    }

    @Override
    public String getName(final UID uniqueId) {
        Preconditions.checkNotNull(uniqueId, "A null uniqueId is not valid");

        // have to wrap the byte[] so that we get a equals/hashcode that looks
        // at the content rather than the instance
        // TODO Consider using Bytes.mapKey instead
        final String name = Try.of(() -> uidToNameCache.get(uniqueId))
                .getOrElseThrow(() -> new RuntimeException(String.format(
                    "uniqueId %s should exist in the cache, something may have gone wrong with self population",
                        uniqueId.toAllForms())));

        if (name == null) {
            throw new RuntimeException(String.format(
                    "uniqueId %s has a null value associated with it in the cache, something has gone wrong with the UID cache/tables as all UIDs should have a name",
                    uniqueId.toAllForms()));
        }

        return name;
    }

    @Override
    public int getWidth() {
        return uniqueId.getWidth();
    }

    @Override
    public int getCacheSize() {
        throw new UnsupportedOperationException("TODO, ehCache v3.2 doesn't seem to support this");
    }

    private static class NameToUidLoaderWriter extends AbstractReadOnlyCacheLoaderWriter<String, UID> {
        private final UniqueId uniqueId;

        @Inject
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

    private static class UidToNameLoaderWriter extends AbstractReadOnlyCacheLoaderWriter<UID, String> {
        private final UniqueId uniqueId;

        @Inject
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
}
