

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

import com.google.common.base.Preconditions;
import javaslang.control.Try;
import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.cache.CacheFactory;

import javax.inject.Inject;
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
                    //not in cache or table so create it in the tables
                    //in the event that another thread does this as well then getOrCreateId will handle
                    //thread safety issues
                    UID newUid = UID.from(uniqueId.getOrCreateId(name));
                    //add the new K/V to both caches as we have the values and chances are somebody else will need them
                    uidToNameCache.put(newUid, name);
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

}
