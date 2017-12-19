package stroom.stats.test;

import javaslang.control.Try;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.StroomStatsStoreEntity;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class StroomStatsStoreEntityHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(StroomStatsStoreEntityHelper.class);

    public static StatisticConfiguration createDummyStatisticConfiguration(
            final String tag,
            final StatisticType statisticType,
            final EventStoreTimeIntervalEnum interval,
            final String... fields){

        String statNameStr = tag + Instant.now().toString() + "-" + statisticType + "-" + interval;
        String statUuidStr = StatisticsHelper.getUuidKey(statNameStr);
        LOGGER.info("Creating stat name : {}", statNameStr);
        StatisticConfiguration statisticConfiguration = new StroomStatsStoreEntityBuilder(
                statUuidStr,
                statNameStr,
                statisticType,
                interval,
                StatisticRollUpType.ALL)
                .addFields(fields)
                .build();

        return statisticConfiguration;
    }

    public static List<String> persistDummyStatisticConfigurations(
            final List<StatisticConfiguration> statisticConfigurations,
            final SessionFactory sessionFactory,
            final StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller) {

        return statisticConfigurations.stream()
                .map(statisticConfiguration -> {
                    Try<StatisticConfiguration> entity = StroomStatsStoreEntityHelper.addStatConfig(
                            sessionFactory,
                            stroomStatsStoreEntityMarshaller,
                            statisticConfiguration);

                    return entity.get().getUuid();
                })
                .collect(Collectors.toList());
    }

    public static Try<StatisticConfiguration> addStatConfig(
            SessionFactory sessionFactory,
            StroomStatsStoreEntityMarshaller stroomStatsStoreEntityMarshaller,
            StatisticConfiguration statisticConfiguration) {

        StroomStatsStoreEntity entity = (StroomStatsStoreEntity) statisticConfiguration;

        try (Session session = sessionFactory.openSession()){
            ManagedSessionContext.bind(session);
            Transaction transaction = session.beginTransaction();

            WriteOnlyStroomStatsStoreEntityDAO statConfDao = new WriteOnlyStroomStatsStoreEntityDAO(
                    sessionFactory,
                    stroomStatsStoreEntityMarshaller);

            LOGGER.debug("Persisting statConfig {} with uuid {}", entity.getName(), entity.getUuid());
            StroomStatsStoreEntity persistedEntity;
            try {
                persistedEntity = statConfDao.persist(entity);
            } catch (HibernateException e) {
                throw new RuntimeException(String.format("Error persisting statConfig %s, %s", entity.getName(), e.getMessage()), e);
            }
            LOGGER.debug("Persisted statConfig {} with uuid {}", entity.getName(), entity.getUuid());

            transaction.commit();
            return Try.success(persistedEntity);
        } catch (Exception e) {
            return Try.failure(e);
        }
    }
}
