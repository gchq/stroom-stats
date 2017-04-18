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

import com.google.inject.Injector;
import org.assertj.core.api.Assertions;
import org.ehcache.CacheManager;
import org.hibernate.SessionFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticConfigurationEntityHelper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

public class TestStatisticConfigurationEntityDAOImpl extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestStatisticConfigurationEntityDAOImpl.class);

    private Injector injector = getApp().getInjector();
    private SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
    private StatisticConfigurationEntityMarshaller marshaller = injector.getInstance(StatisticConfigurationEntityMarshaller.class);

    private StatisticConfigurationService statisticConfigurationService = injector.getInstance(StatisticConfigurationService.class);

    @Test
    public void loadByName() throws Exception {
        StatisticConfiguration entity1 = createStatisticConfigurationEntity("statConfig1");

        //Ensure the cache is clear to make sure it uses the loaderWriter to pull from DB
        clearCache(StatisticConfigurationServiceImpl.KEY_BY_UUID_CACHE_NAME);

        StatisticConfiguration foundEntity = statisticConfigurationService.fetchStatisticConfigurationByUuid(entity1.getUuid())
                .orElseThrow(() -> new RuntimeException(String.format("Entity %s should exist", entity1)));

        Assertions.assertThat(foundEntity).isEqualTo(entity1);

        //now do it again, which should just come straight from the cache

        foundEntity = statisticConfigurationService.fetchStatisticConfigurationByUuid(entity1.getUuid())
                .orElseThrow(() -> new RuntimeException(String.format("Entity %s should exist", entity1)));

        Assertions.assertThat(foundEntity).isEqualTo(entity1);
    }

    @Test
    public void loadByUuid() throws Exception {
        StatisticConfiguration entity1 = createStatisticConfigurationEntity("statConfig1");

        //Ensure the cache is clear to make sure it uses the loaderWriter to pull from DB
        clearCache(StatisticConfigurationServiceImpl.KEY_BY_NAME_CACHE_NAME);

        StatisticConfiguration foundEntity = statisticConfigurationService.fetchStatisticConfigurationByName(entity1.getName())
                .orElseThrow(() -> new RuntimeException(String.format("Entity %s should exist", entity1)));

        Assertions.assertThat(foundEntity).isEqualTo(entity1);

        //now do it again, which should just come straight from the cache

        foundEntity = statisticConfigurationService.fetchStatisticConfigurationByName(entity1.getName())
                .orElseThrow(() -> new RuntimeException(String.format("Entity %s should exist", entity1)));

        Assertions.assertThat(foundEntity).isEqualTo(entity1);
    }

    @Test
    public void loadByUuidAndName_volumeTest() throws Exception {
        clearCache(StatisticConfigurationServiceImpl.KEY_BY_UUID_CACHE_NAME);

        List<StatisticConfiguration> entities = new ArrayList<>();
        //persist 1_500 entities
        IntStream.rangeClosed(1, 1_500).forEach(i ->
                entities.add(createStatisticConfigurationEntity("volTest" + i)));

        //Now have 2 goes at retrieving all 1_500 entities. As the Cache doesn't hold that many it
        // will mean it will have to keep evicting entities from the cache and loading new ones
        IntStream.rangeClosed(1,2).forEach(i -> {

            List<StatisticConfiguration> shuffledEntities = new ArrayList<>(entities);
            Collections.shuffle(shuffledEntities);

            LOGGER.debug("Iteration {}", i);
            shuffledEntities.forEach(entity -> {

                StatisticConfiguration foundEntity = statisticConfigurationService.fetchStatisticConfigurationByUuid(entity.getUuid())
                        .orElseThrow(() -> new RuntimeException(String.format("Entity %s should exist", entity)));

                Assertions.assertThat(foundEntity).isEqualTo(entity);

                foundEntity = statisticConfigurationService.fetchStatisticConfigurationByName(entity.getName())
                        .orElseThrow(() -> new RuntimeException(String.format("Entity %s should exist", entity)));

                Assertions.assertThat(foundEntity).isEqualTo(entity);
            });
        });
    }

    @Test
    public void loadAll() throws Exception {
        StatisticConfiguration entity1 = createStatisticConfigurationEntity("statConfig1");
        StatisticConfiguration entity2 = createStatisticConfigurationEntity("statConfig2");
        StatisticConfiguration entity3 = createStatisticConfigurationEntity("statConfig3");

        List<StatisticConfiguration> entities = statisticConfigurationService.fetchAll();

        Assertions.assertThat(entities).contains(entity1, entity2, entity3);

        entities = statisticConfigurationService.fetchAll();

        Assertions.assertThat(entities).contains(entity1, entity2, entity3);
    }

    private StatisticConfiguration createStatisticConfigurationEntity(String prefix) {

        return StatisticConfigurationEntityHelper.addStatConfig(
                sessionFactory,
                marshaller,
                new StatisticConfigurationEntityBuilder(prefix + Instant.now().toString(),
                        StatisticType.COUNT,
                        1_000,
                        StatisticRollUpType.ALL).build()
        )
                .onFailure(e -> {
                    throw new RuntimeException(e);
                }).get();
    }

    private void clearCache(String name) {
        injector.getInstance(CacheManager.class).getCache(name, String.class, StatisticConfiguration.class).clear();
    }

}