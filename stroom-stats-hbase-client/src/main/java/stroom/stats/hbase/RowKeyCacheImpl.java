

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

package stroom.stats.hbase;

import org.ehcache.Cache;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.TimeAgnosticStatisticEvent;
import stroom.stats.cache.AbstractReadOnlyCacheLoaderWriter;
import stroom.stats.cache.CacheFactory;
import stroom.stats.hbase.structure.TimeAgnosticRowKey;
import stroom.stats.hbase.uid.UniqueIdCache;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;

@Singleton
public class RowKeyCacheImpl implements RowKeyCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowKeyCacheImpl.class);

    static final String EVENT_TO_ROW_KEY_CACHE_NAME = "eventToRowKeyCache";
    private final Cache<TimeAgnosticStatisticEvent, TimeAgnosticRowKey> rowKeyCache;

    @Inject
    public RowKeyCacheImpl(final CacheFactory cacheFactory,
                           final RowKeyLoaderWriter rowKeyLoaderWriter) {

        this.rowKeyCache = cacheFactory.getOrCreateCache(EVENT_TO_ROW_KEY_CACHE_NAME, TimeAgnosticStatisticEvent.class,
                TimeAgnosticRowKey.class, Optional.of(rowKeyLoaderWriter));
    }

    @Override
    public TimeAgnosticRowKey getTimeAgnosticRowKey(final TimeAgnosticStatisticEvent timeAgnosticStatisticEvent) {
        LOGGER.trace("Row key cache get called for event [{}]", timeAgnosticStatisticEvent);

        // converts the passed event into a time agnostic form and looks up the
        // resulting object in the row key cache
        // to see if we have already built a time agnostic row key for that time
        // agnostic event. If not found then the
        // cache will construct a rowkey and self populate itself.
        return rowKeyCache.get(timeAgnosticStatisticEvent);
    }

    private static class RowKeyLoaderWriter extends AbstractReadOnlyCacheLoaderWriter<TimeAgnosticStatisticEvent, TimeAgnosticRowKey> {

        private static final Logger LOGGER = LoggerFactory.getLogger(RowKeyLoaderWriter.class);

        private final UniqueIdCache uniqueIdCache;

        @Inject
        public RowKeyLoaderWriter(final UniqueIdCache uniqueIdCache) {
            LOGGER.info("Initialising RowKeyLoaderWriter");
            this.uniqueIdCache = uniqueIdCache;
        }

        @Override
        public TimeAgnosticRowKey load(final TimeAgnosticStatisticEvent key) throws Exception {
            return RowKeyCache.getTimeAgnosticRowKeyUncached(key, uniqueIdCache);
        }

        @Override
        public Map<TimeAgnosticStatisticEvent, TimeAgnosticRowKey> loadAll(final Iterable<? extends TimeAgnosticStatisticEvent> keys) throws BulkCacheLoadingException, Exception {
            throw new UnsupportedOperationException("LoadAll is not currently supported on this cache");
        }

    }
}
