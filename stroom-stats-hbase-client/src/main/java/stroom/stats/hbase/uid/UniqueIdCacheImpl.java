

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
import stroom.stats.cache.CacheFactory;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.util.Optional;

public class UniqueIdCacheImpl implements UniqueIdCache {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(UniqueIdCacheImpl.class);

    static final String NAME_TO_UID_CACHE_NAME = "nameToUidCache";
    static final String UID_TO_NAME_CACHE_NAME = "uidToNameCache";

    private final UniqueIdGenerator uniqueIdGenerator;

    private final Cache<String, UID> nameToUidCache;
    private final Cache<UID, String> uidToNameCache;

    @Inject
    public UniqueIdCacheImpl(final UniqueIdGenerator uniqueIdGenerator,
                             final CacheFactory cacheFactory,
                             final NameToUidLoaderWriter nameToUidLoaderWriter,
                             final UidToNameLoaderWriter uidToNameLoaderWriter) {

        this.uniqueIdGenerator = uniqueIdGenerator;

        this.nameToUidCache = cacheFactory.getOrCreateCache(NAME_TO_UID_CACHE_NAME, String.class, UID.class, Optional.of(nameToUidLoaderWriter));
        this.uidToNameCache = cacheFactory.getOrCreateCache(UID_TO_NAME_CACHE_NAME, UID.class, String.class, Optional.of(uidToNameLoaderWriter));
    }

    @Override
    public UID getOrCreateId(final String name) {
        Preconditions.checkNotNull(name, "A null name is not valid");

        //will either get the UID from the cache or use the loaderwriter to look it up in hbase
        //but it won't create a new UID.
        UID uidFromCache = nameToUidCache.get(name);

        if (uidFromCache == null) {
            //not in cache or table so create it in the tables
            //in the event that another thread does this as well then getOrCreateId and HBase's consistency
            //guarantees will handle thread safety issues
            UID newUid = uniqueIdGenerator.getOrCreateId(name);
            Preconditions.checkNotNull(newUid,
                    "newUid should never be null as we are creating one for name ", name);

            LOGGER.trace(() -> String.format("Created new UID %s for name %s", newUid.toString(), name));

            //add the new K/V to both caches as we have the values and chances are somebody else will need them
            uidToNameCache.put(newUid, name);
            nameToUidCache.put(name, newUid);
            return newUid;
        } else {
            return uidFromCache;
        }
    }

    @Override
    public Optional<UID> getUniqueId(final String name) {
        Preconditions.checkNotNull(name, "A null name is not valid");
        return Optional.ofNullable(nameToUidCache.get(name));
    }

    @Override
    public String getName(final UID uniqueId) {
        Preconditions.checkNotNull(uniqueId, "A null uniqueIdGenerator is not valid");


        final String name = Try
                .of(() ->
                        uidToNameCache.get(uniqueId))
                .getOrElseThrow(() ->
                        //exception will be a failure in the loaderwriter
                        new RuntimeException(String.format(
                                "uniqueIdGenerator %s should exist in the cache, something may have gone wrong with self population",
                                uniqueId.toAllForms())));

        if (name == null) {
            throw new RuntimeException(String.format(
                    "uniqueIdGenerator %s has a null value associated with it in the cache, something has gone wrong with the UID cache/tables as all UIDs should have a name",
                    uniqueId.toAllForms()));
        }

        return name;
    }

    @Override
    public int getWidth() {
        return uniqueIdGenerator.getWidth();
    }

    @Override
    public int getCacheSize() {
        throw new UnsupportedOperationException("TODO, ehCache v3.2 doesn't seem to support this");
    }

}
