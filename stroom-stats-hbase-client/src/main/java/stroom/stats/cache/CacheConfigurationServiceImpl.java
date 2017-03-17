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

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.properties.StroomPropertyService;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CacheConfigurationServiceImpl implements CacheConfigurationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheConfigurationServiceImpl.class);

    static final String PROP_KEY_PREFIX_CACHE = "stroom.stats.cache.";
    static final String PROP_KEY_SUFFIX_MAX_ENTRIES_HEAP = ".maxEntriesHeap";
    static final String PROP_KEY_SUFFIX_MAX_MB_OFF_HEAP = ".maxEntriesOffHeap";
    static final String PROP_KEY_SUFFIX_MAX_MB_DISK = ".maxEntriesDisk";
    static final String PROP_KEY_SUFFIX_TIME_TO_LIVE_SECS = ".timeToLiveSecs";
    static final String PROP_KEY_SUFFIX_TIME_TO_IDLE_SECS = ".timeToIdleSecs";

    private final StroomPropertyService stroomPropertyService;

    @FunctionalInterface
    private interface BuilderFunction<T> {
        T apply(T builder, int value);
    }

    private static final Map<String, BuilderFunction<CacheConfigurationBuilder<?, ?>>> expiryFunctionMap = new HashMap<>();
    private static final Map<String, BuilderFunction<ResourcePoolsBuilder>> entriesFunctionMap = new HashMap<>();

    static {
        //maps to hold the builder method that corresponds to the property suffix
        expiryFunctionMap.put(PROP_KEY_SUFFIX_TIME_TO_IDLE_SECS, (builder, value) ->
                builder.withExpiry(Expirations.timeToIdleExpiration(Duration.of(value, TimeUnit.SECONDS))));
        expiryFunctionMap.put(PROP_KEY_SUFFIX_TIME_TO_LIVE_SECS, (builder, value) ->
                builder.withExpiry(Expirations.timeToLiveExpiration(Duration.of(value, TimeUnit.SECONDS))));

        //Gotcha - a call to .heap(value) always returns a brand new builder so if
        //.heap wasn't called first then any existing builds would be lost
        entriesFunctionMap.put(PROP_KEY_SUFFIX_MAX_ENTRIES_HEAP, (builder, value) ->
                builder.heap(value, EntryUnit.ENTRIES));
        entriesFunctionMap.put(PROP_KEY_SUFFIX_MAX_MB_OFF_HEAP, (builder, value) ->
                builder.offheap(value, MemoryUnit.MB));
        entriesFunctionMap.put(PROP_KEY_SUFFIX_MAX_MB_DISK, (builder, value) ->
                builder.disk(value, MemoryUnit.MB));
    }

    @Inject
    CacheConfigurationServiceImpl(final StroomPropertyService stroomPropertyService) {
        this.stroomPropertyService = stroomPropertyService;
    }

    private OptionalInt getPropertyValue(final String cacheName, final String propertySuffix) {
        return stroomPropertyService.getIntProperty(buildKey(cacheName, propertySuffix));
    }

    static String buildKey(final String cacheName, final String propertySuffix) {
        return PROP_KEY_PREFIX_CACHE + cacheName + propertySuffix;
    }

    @Override
    public <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilder(final String cacheName,
                                                                               final Class<K> keyType,
                                                                               final Class<V> valueType) {

        LOGGER.info("Configuring cache with name {}, keyType {}, valueType {}", cacheName, keyType.getCanonicalName(), valueType.getCanonicalName());
        //using atomic ref to get round resourcePoolsBuilder chaining with immutable builders
        final AtomicReference<ResourcePoolsBuilder> resourcePoolsBuilderRef = new AtomicReference<>(ResourcePoolsBuilder.newResourcePoolsBuilder());

        //for each prop suffix apply the corresponding function to the builder
        entriesFunctionMap.entrySet().forEach(entry -> {
            final String suffix = entry.getKey();
            final BuilderFunction func = entry.getValue();
            getPropertyValue(cacheName, suffix)
                    .ifPresent(val -> {
                        LOGGER.info("Applying configuration property {} and value {} to cache {}", suffix, val, cacheName);
                        final ResourcePoolsBuilder existingBuilder = resourcePoolsBuilderRef.get();
                        resourcePoolsBuilderRef.set((ResourcePoolsBuilder) func.apply(existingBuilder, val));
                    });
        });

        final AtomicReference<CacheConfigurationBuilder<K, V>> cacheConfigurationBuilderRef = new AtomicReference<>(CacheConfigurationBuilder
                .newCacheConfigurationBuilder(keyType, valueType, resourcePoolsBuilderRef.get().build()));

        final AtomicInteger expiryCounter = new AtomicInteger(0);
        expiryFunctionMap.entrySet().forEach(entry -> {
            final String suffix = entry.getKey();
            final BuilderFunction func = entry.getValue();
            getPropertyValue(cacheName, suffix)
                    .ifPresent(val -> {
                        LOGGER.info("Applying configuration property {} and value {} to cache {}", suffix, val, cacheName);
                        cacheConfigurationBuilderRef.set((CacheConfigurationBuilder<K, V>) func.apply(cacheConfigurationBuilderRef.get(), val));
                        if (expiryCounter.incrementAndGet() > 1) {
                            throw new RuntimeException(String.format("Only one expiry setting can be applied to a cache. Cache name %s", cacheName));
                        }
                    });
        });

        //return the build so the caller can chain more methods on it
        return cacheConfigurationBuilderRef.get();
    }

}
