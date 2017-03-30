package stroom.stats.correlation;

import com.google.inject.Injector;
import javaslang.control.Try;
import org.hibernate.SessionFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StatisticConfigurationEntityMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StatisticConfigurationEntityBuilder;
import stroom.stats.test.StatisticConfigurationEntityHelper;

public class StatisticConfigurationCreator {
    public static String create(
            Injector injector,
            String statName,
            StatisticType statisticType,
            EventStoreTimeIntervalEnum interval,
            String... fields){
        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntityBuilder(
                statName,
                statisticType,
                interval.columnInterval(),
                StatisticRollUpType.ALL)
                .addFields(fields)
                .build();

        SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
        StatisticConfigurationEntityMarshaller statisticConfigurationMarshaller = injector.getInstance(StatisticConfigurationEntityMarshaller.class);
        Try<StatisticConfigurationEntity> statisticConfigurationEntityTry = StatisticConfigurationEntityHelper.addStatConfig(
                sessionFactory, statisticConfigurationMarshaller, statisticConfigurationEntity);
        return statisticConfigurationEntityTry.get().getUuid();
    }
}
