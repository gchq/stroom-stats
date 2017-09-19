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
import stroom.stats.partitions.StatEventKeyPartitioner;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.v4.ObjectFactory;
import stroom.stats.schema.v4.Statistics;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.mapping.AbstractStatisticFlatMapper;
import stroom.stats.streams.serde.StatAggregateSerde;
import stroom.stats.streams.serde.StatEventKeySerde;
import stroom.stats.util.logging.LambdaLogger;
import stroom.stats.schema.v4.StatisticsMarshaller;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StatisticsFlatMappingStreamFactory {

    public static final String VALIDATION_ERROR_TEXT = "VALIDATION_ERROR";
    public static final String UNMARSHALLING_ERROR_TEXT = "UNMARSHALLING_ERROR";

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsFlatMappingStreamFactory.class);

    //private final Predicate<StatEventKey, StatAggregate>[] intervalPredicates;

    private interface InterValToPredicateMapper extends Function<IntervalTopicPair, Predicate<StatEventKey, StatAggregate>> {
    }


    //Defined to avoid 'generic array creation' compiler warnings
    private interface UnmarshalledXmlWrapperPredicate extends Predicate<String, UnmarshalledXmlWrapper> {
    }

    //define the predicates for forking a topic into valid and invalid events based on the validity of an UnmarshalledXmlWrapper
    private static UnmarshalledXmlWrapperPredicate[] VALID_INVALID_XML_WRAPPER_BRANCHING_PREDICATES = new UnmarshalledXmlWrapperPredicate[]{
            (String key, UnmarshalledXmlWrapper value) -> value.isValid(),
            StatisticsFlatMappingStreamFactory::catchAllPredicate
    };

    //Defined to avoid 'generic array creation' compiler warnings
    private interface StatisticWrapperPredicate extends Predicate<String, StatisticWrapper> {
    }

    //define the predicates for forking a topic into valid and invalid events based on the validity of a statisticWrapper
    private static StatisticWrapperPredicate[] VALID_INVALID_STAT_WRAPPER_BRANCHING_PREDICATES = new StatisticWrapperPredicate[]{
            (String key, StatisticWrapper value) -> value.isValid(),
            StatisticsFlatMappingStreamFactory::catchAllPredicate
    };

    private final StatisticConfigurationService statisticConfigurationService;
    private final StroomPropertyService stroomPropertyService;
    private final StatisticsMarshaller statisticsMarshaller;

    @Inject
    StatisticsFlatMappingStreamFactory(final StatisticConfigurationService statisticConfigurationService,
                                       final StroomPropertyService stroomPropertyService,
                                       final StatisticsMarshaller statisticsMarshaller) {

        this.statisticConfigurationService = statisticConfigurationService;
        this.stroomPropertyService = stroomPropertyService;
        this.statisticsMarshaller = statisticsMarshaller;

        //Construct a list of predicate functions for
//        intervalPredicates = Arrays.stream(EventStoreTimeIntervalEnum.values())
//                .map((InterValToPredicateMapper) interval ->
//                        (StatEventKey statKey, StatAggregate statAggregate) -> statKey.equalsIntervalPart(interval))
//                .toArray(size -> new Predicate[size]);
    }

    KafkaStreams buildStream(final StreamsConfig streamsConfig, final String inputTopic, final String badEventTopic,
                             final String intervalTopicPrefix,
                             final AbstractStatisticFlatMapper statisticMapper) {

        LOGGER.info("Building stream with input topic {}, badEventTopic {}, intervalTopicPrefix {} and mapper {}",
                inputTopic, badEventTopic, intervalTopicPrefix, statisticMapper.getClass().getSimpleName());

        Serde<String> stringSerde = Serdes.String();
        Serde<StatEventKey> statKeySerde = StatEventKeySerde.instance();
        Serde<StatAggregate> statAggregateSerde = StatAggregateSerde.instance();

        KStreamBuilder builder = new KStreamBuilder();
        //This is the input to all the processing, key is the uuid of the stat, value is the stat XML
        KStream<String, String> inputStream = builder.stream(stringSerde, stringSerde, inputTopic);

        //currently the stat uuid is both the msg key and in the Statistic object.
        //This does mean duplication but means the msg can exist without the key, without losing meaning
        //TODO In future if we have msgs conforming to different versions of the schema then we may have to inspect the
        //namespace in the msg and use the appropriate unMarshaller for that version
        KStream<String, UnmarshalledXmlWrapper>[] unmarshallingForks = inputStream
                .filter((key, value) -> {
                    //like a peek function
                    //use with caution as the messages could be very frequent and large
                    LOGGER.trace("Received {} : {}", key, value);
                    return true;
                })
                .mapValues(this::unmarshallXml) //attempt to unmarshal the xml string to classes
                .branch(VALID_INVALID_XML_WRAPPER_BRANCHING_PREDICATES); //split out the ones that failed unmarshalling

        KStream<String, UnmarshalledXmlWrapper> validUnmarshalledXml = unmarshallingForks[0];
        KStream<String, UnmarshalledXmlWrapper> invalidUnmarshalledXml = unmarshallingForks[1];

        //Send the bad events out to a bad topic as the original xml with the error message attached to the
        // bottom of the XML, albeit as individual events rather than batches
        invalidUnmarshalledXml
                .mapValues(this::badStatisticWrapperToString)
                .to(stringSerde, stringSerde, badEventTopic);

        KStream<String, StatisticWrapper>[] statWrapperForks = validUnmarshalledXml
                .flatMapValues(unmarshalledXmlWrapper ->
                        unmarshalledXmlWrapper.getStatistics().getStatistic()) //flatMap a batch of stats down to individual events, badly named jaxb objects
                .map(this::buildStatisticWrapper) //wrap the stat event with its stat config
                .map(StatisticValidator::validate) //validate each one then branch off the bad ones
                .branch(VALID_INVALID_STAT_WRAPPER_BRANCHING_PREDICATES); //fork stream on valid/invalid state of the statisticWrapper

        KStream<String, StatisticWrapper> validStatWrappers = statWrapperForks[0];
        KStream<String, StatisticWrapper> invalidStatWrappers = statWrapperForks[1];

        //Send the bad events out to a bad topic as the original xml with the error message attached to the
        // bottom of the XML, albeit as individual events rather than batches
        invalidStatWrappers
                .mapValues(this::badStatisticWrapperToString)
                .to(stringSerde, stringSerde, badEventTopic);

        //build a list of mappings from interval to topic name.  Not done with a Map as we need to
        //access the collection by position. The order of this list does not matter but it must match
        //the order of the intervalPredicates array below so that position N corresponds to the same
        //interval in both collections, else the stream branching will not work
        List<IntervalTopicPair> intervalTopicPairs = getIntervalTopicPairs(intervalTopicPrefix);

        Predicate<StatEventKey, StatAggregate>[] intervalPredicates = getPredicates(intervalTopicPairs);

        //Ignore any events that are outside the retention period as they would just get deleted in the next
        // purge otherwise. Flatmap each statistic event to a set of statKey/statAggregate pairs,
        //one for each roll up permutation. Then branch the stream into multiple streams, one stream per interval
        //i.e. events with hour granularity go to hour stream (and ultimately topic)
        KStream<StatEventKey, StatAggregate>[] intervalStreams = validStatWrappers
                .filter(this::isInsideLargestPurgeRetention) //ignore too old events
                .flatMap(statisticMapper::flatMap) //map to StatEventKey/StatAggregate pair
                .branch(intervalPredicates);

        //Following line if uncommented can be useful for debugging
//        final ConcurrentMap<EventStoreTimeIntervalEnum, AtomicLong> counters = new ConcurrentHashMap<>();
        //Route each from the stream interval specific branches to the appropriate topic
        for (int i = 0; i < intervalStreams.length; i++) {
            String topic = intervalTopicPairs.get(i).getTopic();
            intervalStreams[i]
                    .filter((key, value) -> {
//                        This is in effect a peek operation for debugging as it always returns true
//                        counters.computeIfAbsent(key.getInterval(), interval -> new AtomicLong(0)).incrementAndGet();
//                        LOGGER.info(String.format("interval %s class %s cumCount %s",
//                                key.getInterval(), value.getClass().getName(), counters.get(key.getInterval()).get()));
                        return true;
                    })
                    .to(statKeySerde, statAggregateSerde, StatEventKeyPartitioner.instance(), topic);
        }

        return new KafkaStreams(builder, streamsConfig);
    }

    private UnmarshalledXmlWrapper unmarshallXml(final String messageValue) {
        try {
            Statistics statistics = statisticsMarshaller.unMarshallFromXml(messageValue);
            return UnmarshalledXmlWrapper.wrapValidMessage(statistics);
        } catch (Exception e) {
            return UnmarshalledXmlWrapper.wrapInvalidMessage(messageValue, e);
        }
    }

    private List<IntervalTopicPair> getIntervalTopicPairs(final String intervalTopicPrefix) {
        //get a sorted (by interval ms) list of topic|interval pairs so we can branch the kstream
        return Arrays.stream(EventStoreTimeIntervalEnum.values())
                .map(interval -> new IntervalTopicPair(TopicNameFactory.getIntervalTopicName(intervalTopicPrefix, interval), interval))
                .sorted()
                .collect(Collectors.toList());
    }

    private Predicate<StatEventKey, StatAggregate>[] getPredicates(final List<IntervalTopicPair> intervalTopicPairs) {
        //map the topic|Interval pair to an array of predicates that tests for equality with each interval, i.e.
        //[
        //  statkey interval == SECOND,
        //  statkey interval == MINUTE,
        //  statkey interval == HOUR,
        //  statkey interval == DAY,
        //  ...
        //]
        return intervalTopicPairs.stream()
                .sequential()
                .map((InterValToPredicateMapper) pair ->
                        (StatEventKey statEventKey, StatAggregate statAggregate) -> statEventKey.equalsIntervalPart(pair.getInterval()))
                .toArray(size -> new Predicate[size]);
    }

    private Statistics wrapStatisticWithStatistics(final Statistics.Statistic statistic) {
        Statistics statistics = new ObjectFactory().createStatistics();
        statistics.getStatistic().add(statistic);
        return statistics;
    }

    private String badStatisticWrapperToString(final StatisticWrapper statisticWrapper) {
        Statistics statisticsObj = wrapStatisticWithStatistics(statisticWrapper.getStatistic());
        return appendError(
                statisticsMarshaller.marshallToXml(statisticsObj),
                VALIDATION_ERROR_TEXT,
                () -> statisticWrapper.getValidationErrorMessage().get());
    }

    private String badStatisticWrapperToString(final UnmarshalledXmlWrapper unmarshalledXmlWrapper) {
        return appendError(
                unmarshalledXmlWrapper.getMessageValue(),
                UNMARSHALLING_ERROR_TEXT,
                () -> unmarshalledXmlWrapper.getThrowable().getMessage());
    }

    private String appendError(final String rawXmlValue,
                               final String errorCode,
                               final Supplier<String> errorMsgSupplier) {
        return new StringBuilder()
                .append(rawXmlValue)
                //Append the error message to the bottom of the XML as an XML comment
                .append("\n<!-- ")
                .append(errorCode)
                .append(" - ")
                .append(errorMsgSupplier.get())
                .append(" -->")
                .toString();
    }

    private StatisticWrapper buildStatisticWrapper(final String key, final Statistics.Statistic statistic) {
        if (key != null) {
            Optional<StatisticConfiguration> optStatConfig =
                    statisticConfigurationService.fetchStatisticConfigurationByUuid(key);

            return new StatisticWrapper(statistic, optStatConfig);
        } else {
            LOGGER.warn("Statistic with no UUID");
            return new StatisticWrapper(statistic, Optional.empty());
        }
    }


    /**
     * Method signature to match {@link Predicate}<{@link String}, {@link StatisticWrapper}>
     */
    private boolean isInsideLargestPurgeRetention(
            @SuppressWarnings("unused") final String statName,
            final StatisticWrapper statisticWrapper) {
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

        boolean result = AbstractStatisticFlatMapper.isInsidePurgeRetention(statisticWrapper, biggestInterval, retentionRowIntervals);
        LOGGER.trace("isInsideLargestPurgeRetention == {}", result);
        return result;
    }


    /**
     * A catchall predicate for allowing everything through, used for clarity
     */
    private static boolean catchAllPredicate(
            @SuppressWarnings("unused") final Object key,
            @SuppressWarnings("unused") final Object value) {
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
