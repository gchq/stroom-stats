package stroom.stats.test;

import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import stroom.stats.configuration.StroomStatsStoreEntity;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;

public class WriteOnlyStroomStatsStoreEntityDAO extends AbstractDAO<StroomStatsStoreEntity> {

    private final StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public WriteOnlyStroomStatsStoreEntityDAO(
            final SessionFactory sessionFactory,
            final StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller) {

        super(sessionFactory);
        this.stroomStatsStoreEntityMarshaller = stroomStatsStoreEntityMarshaller;
    }

    @Override
    public StroomStatsStoreEntity persist(final StroomStatsStoreEntity entity) throws HibernateException {
        return super.persist(stroomStatsStoreEntityMarshaller.marshal(entity));
    }
}
