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

import io.dropwizard.hibernate.AbstractDAO;
import io.dropwizard.hibernate.UnitOfWork;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

public class StatisticConfigurationEntityDAOImpl extends AbstractDAO<StatisticConfigurationEntity> implements StatisticConfigurationEntityDAO {
    private final StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public StatisticConfigurationEntityDAOImpl(final SessionFactory sessionFactory,
                                               final StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller) {
        super(sessionFactory);
        this.statisticConfigurationEntityMarshaller = statisticConfigurationEntityMarshaller;
    }

    @Override
    public Optional<StatisticConfigurationEntity> loadByName(final String name) {
        List entities = super.criteria()
                .add(Restrictions.eq("name", name))
                .list();
        return getSingleEntity(entities);
    }

    @Override
    public Optional<StatisticConfigurationEntity> loadByUuid(final String uuid) {
        List entities = super.criteria()
                .add(Restrictions.eq("uuid", uuid))
                .list();
        return getSingleEntity(entities);
    }

    @Override
    public List<StatisticConfigurationEntity> loadAll() {
        List<StatisticConfigurationEntity> entities = null;
        try {
            entities = super.criteria().list();
        } catch (HibernateException e) {
            throw new RuntimeException("Error loading all statisticConfiguration entities", e);
        }
        entities.forEach(entity -> {
            statisticConfigurationEntityMarshaller.unmarshal(entity);
        });
        return entities;
    }

    Optional<StatisticConfigurationEntity> getSingleEntity(List<?> entities) {
        if (entities.size() == 1) {
            StatisticConfigurationEntity statisticConfigurationEntity = (StatisticConfigurationEntity) entities.get(0);
            statisticConfigurationEntityMarshaller.unmarshal(statisticConfigurationEntity);
            return Optional.of(statisticConfigurationEntity);
        } else if (entities.isEmpty()) {
            return Optional.empty();
        } else {
            throw new RuntimeException("Too many entities found");
        }
    }
}
