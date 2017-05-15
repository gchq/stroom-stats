package stroom.stats.test;

import javaslang.Tuple2;
import javaslang.control.Try;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.common.Folder;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StatisticConfigurationEntityHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticConfigurationEntityHelper.class);

    public static Tuple2<StatisticConfigurationEntity, EventStoreTimeIntervalEnum> createDummyStatisticConfiguration(
            String tag, StatisticType statisticType, EventStoreTimeIntervalEnum interval, String... fields){
        String statNameStr = tag + Instant.now().toString() + "-" + statisticType + "-" + interval;
        LOGGER.info("Creating stat name : {}", statNameStr);
        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                statNameStr,
                statisticType,
                interval.columnInterval(),
                StatisticRollUpType.ALL)
                .addFields(fields)
                .build();

        return new Tuple2(statisticConfigurationEntity, interval);
    }

    public static List<String> persistDummyStatisticConfigurations(
            List<Tuple2<StatisticConfigurationEntity, EventStoreTimeIntervalEnum>> statNameMap,
            SessionFactory sessionFactory,
            StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller) {

        List<String> uuids = new ArrayList<>();
        for (Tuple2<StatisticConfigurationEntity, EventStoreTimeIntervalEnum> stat : statNameMap) {
            Try<StatisticConfigurationEntity> entity = StatisticConfigurationEntityHelper.addStatConfig(
                    sessionFactory,
                    statisticConfigurationEntityMarshaller,
                    stat._1());

            uuids.add(entity.get().getUuid());
        }

        return uuids;
    }

    public static Try<StatisticConfigurationEntity> addStatConfig(
            SessionFactory sessionFactory,
            StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller,
            StatisticConfigurationEntity statisticConfigurationEntity) {

        try (Session session = sessionFactory.openSession()){
            ManagedSessionContext.bind(session);
            Transaction transaction = session.beginTransaction();
            Folder folder = statisticConfigurationEntity.getFolder();

            try {
                GenericDAO<Folder> folderDAO = new GenericDAO<>(sessionFactory);

                Optional<Folder> optPersistedFolder = folderDAO.getByName(folder.getName());
                if (!optPersistedFolder.isPresent()) {
                    LOGGER.debug("Folder {} doesn't exist so creating it", folder.getName());
                    optPersistedFolder = Optional.of(folderDAO.persist(folder));
                    LOGGER.debug("Created folder {} with id {}", optPersistedFolder.get().getName(), optPersistedFolder.get().getId());
                } else {
                    LOGGER.debug("Folder {} already exists with id {}", optPersistedFolder.get().getName(), optPersistedFolder.get().getId());
                }

                statisticConfigurationEntity.setFolder(optPersistedFolder.get());

            } catch (HibernateException e) {
                LOGGER.debug("Failed to create folder entity with msg: {}", e.getMessage(), e);
            }

            WriteOnlyStatisticConfigurationEntityDAO statConfDao = new WriteOnlyStatisticConfigurationEntityDAO(
                    sessionFactory,
                    statisticConfigurationEntityMarshaller);

            LOGGER.debug("Persisting statConfig {} with uuid {}", statisticConfigurationEntity.getName(), statisticConfigurationEntity.getUuid());
            StatisticConfigurationEntity persistedStatConfEntity = null;
            try {
                persistedStatConfEntity = statConfDao.persist(statisticConfigurationEntity);
            } catch (HibernateException e) {
                throw new RuntimeException(String.format("Error persisting statConfig %s, %s", statisticConfigurationEntity.getName(), e.getMessage()), e);
            }
            LOGGER.debug("Persisted statConfig {} with uuid {}", statisticConfigurationEntity.getName(), statisticConfigurationEntity.getUuid());

            transaction.commit();
            return Try.success(persistedStatConfEntity);
        } catch (Exception e) {
            return Try.failure(e);
        }
    }
}
