/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.ObjectFactory;
import stroom.stats.schema.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.mapping.AbstractStatisticMapper;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatKeySerde;
import stroom.stats.util.logging.LambdaLogger;
import stroom.stats.xml.StatisticsMarshaller;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

class StatisticsFlatMappingProcessor {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsFlatMappingProcessor.class);

    //private final Predicate<StatKey, StatAggregate>[] intervalPredicates;

    private interface InterValToPredicateMapper extends Function<IntervalTopicPair, Predicate<StatKey, StatAggregate>> {
    }

    private final StatisticConfigurationService statisticConfigurationService;
    private final StroomPropertyService stroomPropertyService;
    private final StatisticsMarshaller statisticsMarshaller;

    @Inject
    StatisticsFlatMappingProcessor(final StatisticConfigurationService statisticConfigurationService,
                                   final StroomPropertyService stroomPropertyService,
                                   final StatisticsMarshaller statisticsMarshaller) {
        this.statisticConfigurationService = statisticConfigurationService;
        this.stroomPropertyService = stroomPropertyService;
        this.statisticsMarshaller = statisticsMarshaller;

        //Construct a list of predicate functions for
//        intervalPredicates = Arrays.stream(EventStoreTimeIntervalEnum.values())
//                .map((InterValToPredicateMapper) interval ->
//                        (StatKey statKey, StatAggregate statAggregate) -> statKey.equalsIntervalPart(interval))
//                .toArray(size -> new Predicate[size]);
    }

    KafkaStreams buildStream(final StreamsConfig streamsConfig, final String inputTopic, final String badEventTopic,
                             final String intervalTopicPrefix,
                             final AbstractStatisticMapper statisticMapper) {

        LOGGER.info("Building stream with input topic {}, badEventTopic {} and intervalTopicPrefix {}", inputTopic, badEventTopic, intervalTopicPrefix);

        Serde<String> stringSerde = Serdes.String();
        Serde<StatKey> statKeySerde = StatKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> inputStream = builder.stream(stringSerde, stringSerde, inputTopic);

        //TODO currently the stat name is both the msg key and in the Statistic object.
        //Should probably just be in the key
        KStream<String, StatisticWrapper>[] forkedStreams = inputStream
                .mapValues(statisticsMarshaller::unMarshallXml)
                .flatMapValues(Statistics::getStatistic) //flatMap a batch of stats down to individual events, badly named jaxb objects
                .mapValues(this::buildStatisticWrapper) //wrap the stat event with its stat config
                .map(StatisticValidator::validate) //validate each one then branch off the bad ones
                .branch(
                        (key, value) -> value.isValid(),
                        this::catchAll);

        KStream<String, StatisticWrapper> validEvents = forkedStreams[0];
        KStream<String, StatisticWrapper> badEvents = forkedStreams[1];

        //Send the bad events out to a bad topic as the original xml with the error message attached to the
        // bottom of the XML, albeit as individual events rather than batches
        badEvents
                .mapValues(this::badStatisticWrapperToString)
                .to(stringSerde, stringSerde, badEventTopic);

        //build a list of mappings from interval to topic name.  Not done with a Map as we need to
        //access the collection by position. The order of this list does not matter but it must match
        //the order of the intervalPredicates array below so that position N corresponds to the same
        //interval in both collections, else the stream branching will not work
        List<IntervalTopicPair> intervalTopicPairs = getIntervalTopicPairs(intervalTopicPrefix);

        Predicate<StatKey, StatAggregate>[] intervalPredicates = getPredicates(intervalTopicPairs);

        //Ignore any events that are outside the retention period as they would just get deleted in the next
        // purge otherwise. Flatmap each statistic event to a set of statKey/statAggregate pairs,
        //one for each roll up permutation. Then branch the stream into multiple streams, one stream per interval
        //i.e. events with hour granularity go to hour stream (and ultimately topic)
        KStream<StatKey, StatAggregate>[] intervalStreams = validEvents
                .filter(this::isInsideLargestPurgeRetention) //ignore too old events
                .flatMap(statisticMapper::flatMap) //map to StatKey/StatAggregate pair
                .branch(intervalPredicates);

//        final ConcurrentMap<EventStoreTimeIntervalEnum, AtomicLong> counters = new ConcurrentHashMap<>();
        //Route each from the stream interval specific branches to the appropriate topic
        for (int i = 0; i < intervalStreams.length; i++) {
            intervalStreams[i]
//                    .filter((key, value) -> {
//                        //This is in effect a peek operation for debugging as it always returns true
//                        counters.computeIfAbsent(key.getInterval(), interval -> new AtomicLong(0)).incrementAndGet();
//                        LOGGER.info(String.format("interval %s class %s cumCount %s",
//                                key.getInterval(), value.getClass().getName(), counters.get(key.getInterval()).get()));
//                        return true;
//                    })
                    .to(statKeySerde, statAggregateSerde, intervalTopicPairs.get(i).getTopic());
        }
        return new KafkaStreams(builder, streamsConfig);
    }

    private List<IntervalTopicPair> getIntervalTopicPairs(final String intervalTopicPrefix) {
        //get a sorted (by interval ms) list of topic|interval pairs so we can branch the kstream
        return Arrays.stream(EventStoreTimeIntervalEnum.values())
                .map(interval -> new IntervalTopicPair(TopicNameFactory.getIntervalTopicName(intervalTopicPrefix, interval),interval))
                .sorted()
                .collect(Collectors.toList());
    }

    private Predicate<StatKey, StatAggregate>[] getPredicates(final List<IntervalTopicPair> intervalTopicPairs) {
        //map the topic|Interval pair to an array of predicates that tests for equality with each interval, i.e.
        //[statkey interval == SECOND, statkey interval == MINUTE, statkey interval == HOUR, statkey interval == DAY, ...]
        return intervalTopicPairs.stream()
                .sequential()
                .map((InterValToPredicateMapper) pair ->
                        (StatKey statKey, StatAggregate statAggregate) -> statKey.equalsIntervalPart(pair.getInterval()))
                .toArray(size -> new Predicate[size]);
    }


    private Statistics wrapStatisticWithStatistics(final Statistics.Statistic statistic) {
        Statistics statistics = new ObjectFactory().createStatistics();
        statistics.getStatistic().add(statistic);
        return statistics;
    }

    private String badStatisticWrapperToString(StatisticWrapper statisticWrapper) {
        StringBuilder stringBuilder = new StringBuilder();
        Statistics statisticsObj = wrapStatisticWithStatistics(statisticWrapper.getStatistic());
        stringBuilder.append(statisticsMarshaller.marshallXml(statisticsObj));
        //Append the error message to the bottom of the XML as an XML comment
        stringBuilder.append("\n<!-- VALIDATION_ERROR - " + statisticWrapper.getValidationErrorMessage().get() + " -->");
        return stringBuilder.toString();
    }


    private StatisticWrapper buildStatisticWrapper(final Statistics.Statistic statistic) {
        Optional<StatisticConfiguration> optStatConfig = statisticConfigurationService.fetchStatisticConfigurationByName(statistic.getName());

        return new StatisticWrapper(statistic, optStatConfig);
    }


    private boolean isInsideLargestPurgeRetention(final String statName, final StatisticWrapper statisticWrapper) {
        //TODO get smallest interval from stat config, get purge retention for that interval
        //check it is inside it. May want to cache retention periods by interval

        EventStoreTimeIntervalEnum biggestInterval = EventStoreTimeIntervalHelper.getLargestInterval();

        //TODO probably ought to cache this to save computing it each time
        //i.e. a cache of ESTIE:Integer with a short retention, e.g. a few mins
        //TODO this makes the assumption that the biggest interval has the longest retention
        //may be reasonable, maybe not
        String purgeRetentionPeriodsPropertyKey = HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                + biggestInterval.name().toLowerCase();

        final int retentionRowIntervals = stroomPropertyService.getIntPropertyOrThrow(purgeRetentionPeriodsPropertyKey);

        boolean result = AbstractStatisticMapper.isInsidePurgeRetention(statisticWrapper, biggestInterval, retentionRowIntervals);
        LOGGER.trace("isInsideLargestPurgeRetention == {}", result);
        return result;
    }


    /**
     * A catchall predicate for allowing everything through, used for clarity
     */
    private boolean catchAll(final String statname, final StatisticWrapper statisticWrapper) {
        return true;
    }

    private static class IntervalTopicPair implements Comparable<IntervalTopicPair> {
       private final String topic;
       private final EventStoreTimeIntervalEnum interval;

        public IntervalTopicPair(final String topic, final EventStoreTimeIntervalEnum interval) {
            this.topic = topic;
            this.interval = interval;
        }

        public String getTopic() {
            return topic;
        }

        public EventStoreTimeIntervalEnum getInterval() {
            return interval;
        }

        @Override
        public int compareTo(final IntervalTopicPair that) {
            return Long.compare(this.interval.columnInterval(), that.interval.columnInterval());
        }
    }

}
