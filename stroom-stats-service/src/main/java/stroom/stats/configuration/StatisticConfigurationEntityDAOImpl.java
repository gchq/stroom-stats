/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.configuration;

import io.dropwizard.hibernate.AbstractDAO;
import io.dropwizard.hibernate.UnitOfWork;
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
        List<StatisticConfigurationEntity> entities = super.criteria().list();
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
