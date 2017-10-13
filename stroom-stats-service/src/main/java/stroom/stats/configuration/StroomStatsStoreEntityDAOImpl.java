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
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import javax.persistence.NoResultException;
import java.util.List;
import java.util.Optional;

public class StroomStatsStoreEntityDAOImpl
        extends AbstractDAO<StroomStatsStoreEntity>
        implements StroomStatsStoreEntityDAO {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StroomStatsStoreEntityDAOImpl.class);

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
    public Optional<StroomStatsStoreEntity> loadByUuid(final String uuid) {
        LOGGER.trace("loadByUuid called for uuid {}", uuid);
        try {
            StroomStatsStoreEntity entity = super.currentSession()
                    .createQuery(
                            "from StroomStatsStoreEntity " +
                                    "where uuid = :pUuid",
                            StroomStatsStoreEntity.class)
                    .setParameter("pUuid", uuid)
                    .getSingleResult();

            LOGGER.trace("Returning StroomStatsStoreEntity {} for uuid {}", entity, uuid);
            return unmarshalEntity(entity);

        } catch (NoResultException nre) {
            LOGGER.debug("No entity found for uuid {}, returning empty Optional", uuid);
            return Optional.empty();
        } catch (HibernateException e) {
            throw new RuntimeException("Error loading statisticConfiguration with UUID " + uuid, e);
        }
    }

    @Override
    public List<StroomStatsStoreEntity> loadAll() {
        List<StroomStatsStoreEntity> entities;
        try {
            entities = super.currentSession()
                    .createQuery(
                            "from StroomStatsStoreEntity ",
                            StroomStatsStoreEntity.class)
                    .getResultList();
        } catch (HibernateException e) {
            throw new RuntimeException("Error loading all statisticConfiguration entities", e);
        }
        entities.forEach(entity -> {
            stroomStatsStoreEntityMarshaller.unmarshal(entity);
        });
        LOGGER.trace(() -> String.format("Returning %s StroomStatsStoreEntities", entities.size()));
        return entities;
    }

    private Optional<StroomStatsStoreEntity> unmarshalEntity(final StroomStatsStoreEntity entity) {
        if (entity != null) {
            stroomStatsStoreEntityMarshaller.unmarshal(entity);
            return Optional.of(entity);
        } else {
            return Optional.empty();
        }
    }
}
