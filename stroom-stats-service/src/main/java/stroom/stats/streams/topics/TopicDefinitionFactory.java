package stroom.stats.streams.topics;

import javaslang.Tuple;
import javaslang.Tuple2;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TopicDefinitionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicDefinitionFactory.class);

    public static final String PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.statisticEventsPrefix";
    public static final String PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX = "stroom.stats.topics.badStatisticEventsPrefix";
    public static final String PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX = "stroom.stats.topics.statisticRollupPermsPrefix";

    public static final String DELIMITER = "-";

    private final StroomPropertyService stroomPropertyService;

    private final Map<Tuple2<StatisticType,EventStoreTimeIntervalEnum>, TopicDefinition<StatEventKey, StatAggregate>>
            aggregateTopicsMap = new HashMap<>();

    private final Map<StatisticType, TopicDefinition<String, String>> statisticEventTopicsMap = new HashMap<>();
    private final Map<StatisticType, TopicDefinition<String, String>> badStatisticEventTopicsMap = new HashMap<>();

    @Inject
    public TopicDefinitionFactory(final StroomPropertyService stroomPropertyService) {
        this.stroomPropertyService = stroomPropertyService;
        initialiseTopicMaps();
    }

    private void initialiseTopicMaps() {
        // build the topic definitions for the input, bad and aggregate topics
        for (final StatisticType statisticType : StatisticType.values()) {
            statisticEventTopicsMap.put(
                    statisticType,
                    createStatTypedTopic(PROP_KEY_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType));

            badStatisticEventTopicsMap.put(
                    statisticType,
                    createStatTypedTopic(PROP_KEY_BAD_STATISTIC_EVENTS_TOPIC_PREFIX, statisticType));

            for (final EventStoreTimeIntervalEnum interval : EventStoreTimeIntervalEnum.values()) {
                aggregateTopicsMap.put(
                        Tuple.of(statisticType, interval),
                        createStatTypedIntervalTopic(PROP_KEY_STATISTIC_ROLLUP_PERMS_TOPIC_PREFIX, statisticType, interval));
            }
        }
    }

    public TopicDefinition<String, String> getStatisticEventsTopic(final StatisticType statisticType) {
        return Objects.requireNonNull(statisticEventTopicsMap.get(statisticType));
    }

    public TopicDefinition<String, String> getBadStatisticEventsTopic(final StatisticType statisticType) {
        return Objects.requireNonNull(badStatisticEventTopicsMap.get(statisticType));
    }

    public TopicDefinition<StatEventKey, StatAggregate> getAggregatesTopic(final StatisticType statisticType,
                                                                           final EventStoreTimeIntervalEnum interval) {
        return Objects.requireNonNull(aggregateTopicsMap.get(Tuple.of(statisticType, interval)));
    }

    public Map<Tuple2<StatisticType, EventStoreTimeIntervalEnum>, TopicDefinition<StatEventKey, StatAggregate>> getAggregateTopicsMap() {
        return aggregateTopicsMap;
    }

    public Map<StatisticType, TopicDefinition<String, String>> getStatisticEventTopicsMap() {
        return statisticEventTopicsMap;
    }

    public Map<StatisticType, TopicDefinition<String, String>> getBadStatisticEventTopicsMap() {
        return badStatisticEventTopicsMap;
    }

    private TopicDefinition<String, String> createStatTypedTopic(final String topicPrefixPropKey,
                                                                 final StatisticType statisticType) {
        final String topicPrefix = stroomPropertyService.getPropertyOrThrow(topicPrefixPropKey);
        final String topicName = getStatisticTypedName(topicPrefix, statisticType);
        return new TopicDefinition<>(topicName, Serdes.String(), Serdes.String());
    }

    private TopicDefinition<StatEventKey, StatAggregate> createStatTypedIntervalTopic(final String topicPrefixPropKey,
                                                                                      final StatisticType statisticType,
                                                                                      final EventStoreTimeIntervalEnum interval) {
        final String topicPrefix = stroomPropertyService.getPropertyOrThrow(topicPrefixPropKey);
        final String topicName = getIntervalTopicName(getStatisticTypedName(topicPrefix, statisticType), interval);
        return new TopicDefinition<>(topicName, StatEventKeySerde.instance(), StatAggregateSerde.instance());
    }

    private static String getStatisticTypedName(final String prefix, final StatisticType statisticType) {
        return prefix + DELIMITER + statisticType.getDisplayValue();
    }

    private static String getIntervalTopicName(final String prefix, final EventStoreTimeIntervalEnum interval) {
        return prefix + DELIMITER + interval.shortName();
    }
}
