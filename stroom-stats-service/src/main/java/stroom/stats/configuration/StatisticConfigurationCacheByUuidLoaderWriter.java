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

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;

public class StatisticConfigurationCacheByUuidLoaderWriter implements CacheLoaderWriter<String,StatisticConfiguration>{

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticConfigurationCacheByUuidLoaderWriter.class);


    private final StroomStatsStoreEntityDAO stroomStatsStoreEntityDAO;

    @Inject
    public StatisticConfigurationCacheByUuidLoaderWriter(final StroomStatsStoreEntityDAO stroomStatsStoreEntityDAO) {
        this.stroomStatsStoreEntityDAO = stroomStatsStoreEntityDAO;
    }

    @Override
    public StatisticConfiguration load(final String key) throws Exception {
        LOGGER.trace("load called for key {}", key);
        //EHCache doesn't cache null values so if we can't find a stat config for this uuid,
        //just return null
        StatisticConfiguration statisticConfiguration = stroomStatsStoreEntityDAO.loadByUuid(key).orElse(null);

        LOGGER.trace("Returning statisticConfiguration {}", statisticConfiguration);
        return statisticConfiguration;
    }

    @Override
    public Map<String, StatisticConfiguration> loadAll(final Iterable<? extends String> keys)
            throws Exception {
        throw new UnsupportedOperationException("loadAll (getAll) is not currently supported on this cache");
    }

    @Override
    public void write(final String key, final StatisticConfiguration value) throws Exception {
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }

    @Override
    public void writeAll(final Iterable<? extends Map.Entry<? extends String, ? extends StatisticConfiguration>> entries) throws Exception {
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }

    @Override
    public void delete(final String key) throws Exception {
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }

    @Override
    public void deleteAll(final Iterable<? extends String> keys) throws Exception {
        throw new UnsupportedOperationException("CRUD operations are not currently supported on this cache");
    }
}
