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

package stroom.stats.hbase;

import com.google.inject.Injector;
import io.dropwizard.hibernate.AbstractDAO;
import javaslang.control.Try;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.context.internal.ManagedSessionContext;
import org.hibernate.criterion.Restrictions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.DocRef;
import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.Query;
import stroom.stats.AbstractAppIT;
import stroom.stats.api.StatisticType;
import stroom.stats.api.StatisticsService;
import stroom.stats.common.StatisticDataPoint;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.common.Folder;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.AggregatedEvent;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.util.DateUtil;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class HBaseDataLoadIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDataLoadIT.class);

    Injector injector = getApp().getInjector();
    UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);
    StatisticConfigurationService statisticConfigurationService = injector.getInstance(StatisticConfigurationService.class);
    SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
    CustomStatConfDAO customStatConfDAO = new CustomStatConfDAO(sessionFactory, injector.getInstance(StatisticConfigurationEntityMarshaller.class));

    @Test
    public void test() {
        StatisticsService statisticsService = injector.getInstance(StatisticsService.class);
        UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);

        StatisticType statisticType = StatisticType.COUNT;
        EventStoreTimeIntervalEnum interval = EventStoreTimeIntervalEnum.MINUTE;
        RollUpBitMask rollUpBitMask = RollUpBitMask.ZERO_MASK;
        long timeMs = ZonedDateTime.now().toInstant().toEpochMilli();
        List<AggregatedEvent> aggregatedEvents = new ArrayList<>();

        //Put time in the statName to allow us to re-run the test without an empty HBase
        String statNameStr = this.getClass().getName() + "-test-" + Instant.now().toString();
        String tag1Str = "tag1";
        String tag2Str = "tag2";

        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                statNameStr,
                statisticType,
                interval.columnInterval(),
                StatisticRollUpType.ALL)
                .addFields(tag1Str, tag2Str)
                .build();

        addStatConfig(statisticConfigurationEntity);

        UID statName = uniqueIdCache.getOrCreateId(statNameStr);
        assertThat(statName).isNotNull();

        UID tag1 = uniqueIdCache.getOrCreateId(tag1Str);
        UID tag1val1 = uniqueIdCache.getOrCreateId("tag1val1");
        UID tag2 = uniqueIdCache.getOrCreateId(tag2Str);
        UID tag2val1 = uniqueIdCache.getOrCreateId("tag2val1");

        StatKey statKey = new StatKey( statName,
                rollUpBitMask,
                interval,
                timeMs,
                new TagValue(tag1, tag1val1),
                new TagValue(tag2, tag2val1));

        long statValue = 100L;

        StatAggregate statAggregate = new CountAggregate(statValue);

        AggregatedEvent aggregatedEvent = new AggregatedEvent(statKey, statAggregate);

        aggregatedEvents.add(aggregatedEvent);

        statisticsService.putAggregatedEvents(statisticType, interval, aggregatedEvents);

        ExpressionItem dateExpression = new ExpressionTerm(
                StatisticConfiguration.FIELD_NAME_DATE_TIME,
                ExpressionTerm.Condition.BETWEEN,
                String.format("%s,%s",
                        DateUtil.createNormalDateTimeString(Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()),
                        DateUtil.createNormalDateTimeString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())));

        Query query = new Query(
                new DocRef(StatisticConfigurationEntity.ENTITY_TYPE, statisticConfigurationEntity.getUuid()),
                new ExpressionOperator(true, ExpressionOperator.Op.AND, dateExpression));

        StatisticDataSet statisticDataSet = statisticsService.searchStatisticsData(query, statisticConfigurationEntity);

        assertThat(statisticDataSet).isNotNull();
        assertThat(statisticDataSet).size().isEqualTo(1);
        StatisticDataPoint statisticDataPoint = statisticDataSet.getStatisticDataPoints().stream().findFirst().get();
        assertThat(statisticDataPoint.getCount()).isEqualTo(statValue);
        assertThat(statisticDataPoint.getTags()).size().isEqualTo(statKey.getTagValues().size());
    }



    private Try<StatisticConfigurationEntity> addStatConfig(StatisticConfigurationEntity statisticConfigurationEntity) {


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

            StatisticConfigurationEntity persistedStatConfEntity = customStatConfDAO.persist(statisticConfigurationEntity);

            transaction.commit();
            return Try.success(persistedStatConfEntity);
        } catch (Exception e) {
            return Try.failure(e);
        }
    }


    private static class CustomStatConfDAO extends AbstractDAO<StatisticConfigurationEntity> {

        private final StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller;

        /**
         * Creates a new DAO with a given session provider.
         *
         * @param sessionFactory a session provider
         */
        public CustomStatConfDAO(final SessionFactory sessionFactory, final StatisticConfigurationEntityMarshaller statisticConfigurationEntityMarshaller) {
            super(sessionFactory);
            this.statisticConfigurationEntityMarshaller = statisticConfigurationEntityMarshaller;
        }

        @Override
        protected StatisticConfigurationEntity persist(final StatisticConfigurationEntity entity) throws HibernateException {
            return super.persist(statisticConfigurationEntityMarshaller.marshal(entity));
        }
    }

    private static class  GenericDAO<T> extends AbstractDAO<T> {

        /**
         * Creates a new DAO with a given session provider.
         *
         * @param sessionFactory a session provider
         */
        public GenericDAO(final SessionFactory sessionFactory) {
            super(sessionFactory);
        }

        @Override
        protected T persist(final T entity) throws HibernateException {
            return super.persist(entity);
        }

        @Override
        protected T get(final Serializable id) {
            return super.get(id);
        }

        public Optional<T> getByName(final String name) {
            List entities = super.criteria()
                    .add(Restrictions.eq("name", name))
                    .list();
            if (entities == null || entities.size() == 0) {
                return Optional.empty();
            } else if (entities.size() > 1) {
                throw new RuntimeException(String.format("Name %s was not unique, %s entities found", name, entities.size()));
            } else {
                return Optional.of((T) entities.get(0));
            }
        }
    }

}
