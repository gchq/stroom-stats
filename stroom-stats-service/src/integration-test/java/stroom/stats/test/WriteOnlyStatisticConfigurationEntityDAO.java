package stroom.stats.test;

import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;

public class WriteOnlyStatisticConfigurationEntityDAO extends AbstractDAO<StatisticConfigurationEntity> {

    private final StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public WriteOnlyStatisticConfigurationEntityDAO(final SessionFactory sessionFactory, final StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller) {
        super(sessionFactory);
        this.statisticConfigurationEntityMarshaller = statisticConfigurationEntityMarshaller;
    }

    @Override
    public StatisticConfigurationEntity persist(final StatisticConfigurationEntity entity) throws HibernateException {
        return super.persist(statisticConfigurationEntityMarshaller.marshal(entity));
    }
}
