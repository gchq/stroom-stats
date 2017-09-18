package stroom.stats.correlation;

import com.google.inject.Injector;
import javaslang.control.Try;
import org.hibernate.SessionFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.marshaller.StroomStatsStoreEntityMarshaller;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.test.StroomStatsStoreEntityBuilder;
import stroom.stats.test.StroomStatsStoreEntityHelper;

public class StatisticConfigurationCreator {

    public static String create(
            Injector injector,
            String statUuid,
            String statName,
            StatisticType statisticType,
            EventStoreTimeIntervalEnum interval,
            String... fields){

        StatisticConfiguration statisticConfigurationEntity = new StroomStatsStoreEntityBuilder(
                statUuid,
                statName,
                statisticType,
                interval,
                StatisticRollUpType.ALL)
                .addFields(fields)
                .build();

        SessionFactory sessionFactory = injector.getInstance(SessionFactory.class);
        StroomStatsStoreEntityMarshaller statisticConfigurationMarshaller = injector.getInstance(StroomStatsStoreEntityMarshaller.class);
        Try<StatisticConfiguration> statisticConfigurationEntityTry = StroomStatsStoreEntityHelper.addStatConfig(
                sessionFactory, statisticConfigurationMarshaller, statisticConfigurationEntity);
        return statisticConfigurationEntityTry.get().getUuid();
    }
}
