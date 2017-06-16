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
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

public class StroomStatsStoreEntityDAOImpl
        extends AbstractDAO<StroomStatsStoreEntity>
        implements StroomStatsStoreEntityDAO {

    private final StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public StroomStatsStoreEntityDAOImpl(final SessionFactory sessionFactory,
                                         final StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller) {
        super(sessionFactory);
        this.stroomStatsStoreEntityMarshaller = stroomStatsStoreEntityMarshaller;
    }

    @Override
    public Optional<StroomStatsStoreEntity> loadByName(final String name) {
        List<?> entities;
        try {
            entities = super.criteria()
                    .add(Restrictions.eq("name", name))
                    .list();

        } catch (HibernateException e) {
            throw new RuntimeException("Error loading statisticConfiguration with name " + name, e);
        }
        return getSingleEntity(entities);
    }

    @Override
    public Optional<StroomStatsStoreEntity> loadByUuid(final String uuid) {
        List<?> entities;
        try {
            entities = super.criteria()
                    .add(Restrictions.eq("uuid", uuid))
                    .list();

        } catch (HibernateException e) {
            throw new RuntimeException("Error loading statisticConfiguration with UUID " + uuid, e);
        }
        return getSingleEntity(entities);
    }

    @Override
    public List<StroomStatsStoreEntity> loadAll() {
        List<StroomStatsStoreEntity> entities;
        try {
            entities = super.criteria().list();
        } catch (HibernateException e) {
            throw new RuntimeException("Error loading all statisticConfiguration entities", e);
        }
        entities.forEach(entity -> {
            stroomStatsStoreEntityMarshaller.unmarshal(entity);
        });
        return entities;
    }

    Optional<StroomStatsStoreEntity> getSingleEntity(final List<?> entities) {
        if (entities.size() == 1) {
            StroomStatsStoreEntity statisticConfigurationEntity = (StroomStatsStoreEntity) entities.get(0);
            stroomStatsStoreEntityMarshaller.unmarshal(statisticConfigurationEntity);
            return Optional.of(statisticConfigurationEntity);
        } else if (entities.isEmpty()) {
            return Optional.empty();
        } else {
            throw new RuntimeException("Too many entities found");
        }
    }
}
