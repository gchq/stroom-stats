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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple implementation to hold in memory {@link StatisticConfiguration} entities for
 * use in other services
 */
public class MockStatisticConfigurationService implements StatisticConfigurationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockStatisticConfigurationService.class);

    private final ConcurrentMap<String, StatisticConfiguration> uuidToStatConfMap = new ConcurrentHashMap<>();

    @Override
    public List<StatisticConfiguration> fetchAll() {
        return new ArrayList<>(uuidToStatConfMap.values());
    }

    @Override
    public Optional<StatisticConfiguration> fetchStatisticConfigurationByUuid(final String uuid) {
        return Optional.ofNullable(uuidToStatConfMap.get(uuid));
    }

    /**
     * For use in tests for pre-creating a {@link StatisticConfiguration} for subsequent extraction
     */
    public synchronized MockStatisticConfigurationService addStatisticConfiguration(final StatisticConfiguration statisticConfiguration) {

        String name = statisticConfiguration.getName();
        String uuid = statisticConfiguration.getUuid();

        if (uuidToStatConfMap.get(uuid) != null) {
            throw new RuntimeException(String.format("StatisticConfiguration with uuid %s already exists", uuid));
        }

        uuidToStatConfMap.put(uuid, statisticConfiguration);
        return this;
    }

}
