package stroom.stats.test;

import javaslang.control.Try;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.common.Folder;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;

import java.util.Optional;

public class StatisticConfigurationEntityHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticConfigurationEntityHelper.class);

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

            StatisticConfigurationEntity persistedStatConfEntity = statConfDao.persist(statisticConfigurationEntity);

            transaction.commit();
            return Try.success(persistedStatConfEntity);
        } catch (Exception e) {
            return Try.failure(e);
        }
    }
}
