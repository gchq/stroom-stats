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

package stroom.stats.cache;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import javax.inject.Inject;
import java.util.Optional;

public final class CacheFactoryImpl implements CacheFactory {

    private final CacheManager cacheManager;
    private final CacheConfigurationService cacheConfigurationService;

    @Inject
    public CacheFactoryImpl(final CacheManager cacheManager, final CacheConfigurationService cacheConfigurationService) {
        this.cacheManager = cacheManager;
        this.cacheConfigurationService = cacheConfigurationService;
    }

    /**
     * Gets the named ehCache instance or builds it if it doesn't exist.  Cache configuration comes from the StroomPropertyService
     * and the passed optional CacheLoaderWriter.  If you need a more customised cache then use CacheConfigurationService to get
     * a cacheConfigurationBuilder to further customise.
     */
    @Override
    public <K, V> Cache<K, V> getOrCreateCache(final String cacheName, final Class<K> keyType, final Class<V> valueType,
                                               final Optional<CacheLoaderWriter<K, V>> optLoaderWriter) {

        Cache<K, V> cache = cacheManager.getCache(cacheName, keyType, valueType);
        if (cache == null) {
            //cacheManger is a singleton so all threads can synch on that
            synchronized (cacheManager) {
                cache = cacheManager.getCache(cacheName, keyType, valueType);
                if (cache == null) {
                    //construct the cache
                    try {
                        CacheConfigurationBuilder<K, V> builder = cacheConfigurationService.newCacheConfigurationBuilder(
                                cacheName, keyType, valueType);

                        if (optLoaderWriter.isPresent()) {
                            //withLoaderWriter returns a new instance of builder
                            builder = builder.withLoaderWriter(optLoaderWriter.get());
                        }
                        cache = cacheManager.createCache(cacheName, builder.build());
                    } catch (Exception e) {
                        throw new RuntimeException(String.format(
                                "Error trying to create cache %s: %s. Is all the cache configuration correct in Zookeeper?",
                                cacheName,
                                e.getMessage()), e);
                    }
                }
            }
        }
        return cache;
    }
}
