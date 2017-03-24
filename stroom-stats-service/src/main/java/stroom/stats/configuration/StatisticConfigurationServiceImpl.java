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

package stroom.stats.configuration;

import javaslang.control.Try;
import org.ehcache.Cache;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.cache.CacheFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class StatisticConfigurationServiceImpl implements StatisticConfigurationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticConfigurationServiceImpl.class);

    private static final String KEY_BY_NAME_CACHE_NAME = "nameToStatisticConfigurationCache";
    private static final String KEY_BY_UUID_CACHE_NAME = "uuidToStatisticConfigurationCache";

    private final StatisticConfigurationEntityDAO statisticConfigurationEntityDAO;
    private final Cache<String, StatisticConfiguration> keyByNameCache;
    private final Cache<String, StatisticConfiguration> keyByUuidCache;
    private final SessionFactory sessionFactory;

    @Inject
    public StatisticConfigurationServiceImpl(final CacheFactory cacheFactory,
                                             final StatisticConfigurationEntityDAO statisticConfigurationEntityDAO,
                                             final StatisticConfigurationCacheByNameLoaderWriter byNameLoaderWriter,
                                             final StatisticConfigurationCacheByUuidLoaderWriter byUuidLoaderWriter,
                                             final SessionFactory sessionFactory) {
        this.statisticConfigurationEntityDAO = statisticConfigurationEntityDAO;
        this.sessionFactory = sessionFactory;

        this.keyByNameCache = cacheFactory.getOrCreateCache(KEY_BY_NAME_CACHE_NAME, String.class, StatisticConfiguration.class, Optional.of(byNameLoaderWriter));
        this.keyByUuidCache = cacheFactory.getOrCreateCache(KEY_BY_UUID_CACHE_NAME, String.class, StatisticConfiguration.class, Optional.of(byUuidLoaderWriter));
    }

    @Override
    public List<StatisticConfiguration> fetchAll() {
//        return new ArrayList<>(statisticConfigurationEntityDAO.loadAll());

        return executeInSession(() ->
            new ArrayList<>(statisticConfigurationEntityDAO.loadAll())
        );
    }

    @Override
    public Optional<StatisticConfiguration> fetchStatisticConfigurationByName(final String name) {
        return executeInSession(() ->
            Try.of(() -> keyByNameCache.get(name))
                    .onFailure(throwable -> LOGGER.error("Error fetching key {} from the cache",name, throwable))
                    .toJavaOptional()
        );
//        return Try.of(() -> keyByNameCache.get(name))
//                .onFailure(throwable -> LOGGER.error("Error fetching key {} from the cache",name, throwable))
//                .toJavaOptional();
    }

    @Override
    public Optional<StatisticConfiguration> fetchStatisticConfigurationByUuid(final String uuid) {
        return executeInSession(() ->
            Try.of(() -> keyByNameCache.get(uuid))
                    .onFailure(throwable -> LOGGER.error("Error fetching key {} from the cache",uuid, throwable))
                    .toJavaOptional()
        );
    }

    private <T> T executeInSession(Supplier<T> task) {

        try (Session session = sessionFactory.openSession()){
            ManagedSessionContext.bind(session);
            session.beginTransaction();

            return task.get();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error executing task, %s", e.getMessage()), e);
        }
    }
}
