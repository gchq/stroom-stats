package stroom.stats.streams.topics;

import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatEventKeySerde;

import javax.inject.Inject;

public class TopicDefinitionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicDefinitionFactory.class);

    public static final String PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.statisticEventsPrefix";
    public static final String PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.badStatisticEventsPrefix";
    public static final String PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "stroom.stats.topics.statisticRollupPermsPrefix";

    public static final String DELIMITER = "-";

    private final StroomPropertyService stroomPropertyService;

    @Inject
    public TopicDefinitionFactory(final StroomPropertyService stroomPropertyService) {
        this.stroomPropertyService = stroomPropertyService;
    }

    private TopicDefinition<String, String> createStatTypedTopic(final String topicPrefixPropKey,
                                                                final StatisticType statisticType) {
        final String topicPrefix = stroomPropertyService.getPropertyOrThrow(topicPrefixPropKey);
        final String topicName = getStatisticTypedName(topicPrefix, statisticType);
        return new TopicDefinition<>(topicName, Serdes.String(), Serdes.String());
    }

    public TopicDefinition<String, String> createStatisticEventsTopic(final StatisticType statisticType) {
        return createStatTypedTopic(PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType);
    }

    public TopicDefinition<String, String> createBadStatisticEventsTopic(final StatisticType statisticType) {
        return createStatTypedTopic(PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType);
    }

    private TopicDefinition<StatEventKey, StatAggregate> createStatTypedIntervalTopic(final String topicPrefixPropKey,
                                                                                     final StatisticType statisticType,
                                                                                     final EventStoreTimeIntervalEnum interval) {
        final String topicPrefix = stroomPropertyService.getPropertyOrThrow(topicPrefixPropKey);
        final String topicName = getIntervalTopicName(getStatisticTypedName(topicPrefix, statisticType), interval);
        return new TopicDefinition<>(topicName, StatEventKeySerde.instance(), StatAggregateSerde.instance());
    }

    public TopicDefinition<StatEventKey, StatAggregate> createAggregatesTopic(final StatisticType statisticType,
                                                                              final EventStoreTimeIntervalEnum interval) {
        return createStatTypedIntervalTopic(PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval);
    }

    private static String getStatisticTypedName(final String prefix, final StatisticType statisticType) {
        return prefix + DELIMITER + statisticType.getDisplayValue();
    }

    private static String getIntervalTopicName(final String prefix, final EventStoreTimeIntervalEnum interval) {
        return prefix + DELIMITER + interval.shortName();
    }

}
