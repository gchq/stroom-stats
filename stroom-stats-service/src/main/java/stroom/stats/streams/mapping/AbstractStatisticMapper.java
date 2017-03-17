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

package stroom.stats.streams.mapping;

import org.apache.kafka.streams.KeyValue;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.EventStoreTimeIntervalHelper;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.CompoundIdentifierType;
import stroom.stats.schema.Statistics;
import stroom.stats.schema.TagType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractStatisticMapper {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(AbstractStatisticMapper.class);

    public static final String NULL_VALUE_STRING = "<<<<NULL_VALUE>>>>";

    private final UniqueIdCache uniqueIdCache;
    private final StroomPropertyService stroomPropertyService;
    private final UID rolledUpValue;

    public AbstractStatisticMapper(final UniqueIdCache uniqueIdCache,
                                   final StroomPropertyService stroomPropertyService) {
        this.uniqueIdCache = uniqueIdCache;
        this.stroomPropertyService = stroomPropertyService;

        //can hold this byte value for the life of the application as it will nto change
        rolledUpValue = uniqueIdCache.getOrCreateId(RollUpBitMask.ROLL_UP_TAG_VALUE);
    }


    public abstract Iterable<KeyValue<StatKey, StatAggregate>> flatMap(String statName, StatisticWrapper statisticWrapper);

    private TagValue buildTagValue(String tag, Optional<String> value) {
        LOGGER.trace(() -> String.format("Creating TagValue tag: %s value %s", tag, value.orElse("NULL")));
        UID tagUid = uniqueIdCache.getOrCreateId(tag);
        UID valueUid = uniqueIdCache.getOrCreateId(value.orElse(NULL_VALUE_STRING));
        return new TagValue(tagUid, valueUid);
    }

    protected List<MultiPartIdentifier> convertEventIds(final Statistics.Statistic statistic, final int maxEventIds) {
        if (statistic.getIdentifiers() == null || statistic.getIdentifiers().getCompoundIdentifier() == null) {
            return Collections.emptyList();
        }
        return statistic.getIdentifiers().getCompoundIdentifier().stream()
                .limit(maxEventIds)
                .map(compoundIdentifierType ->
                        new MultiPartIdentifier(compoundIdentifierType.getLongIdentifierOrStringIdentifier().stream()
                                .map(longOrStringId -> {
                                    if (longOrStringId instanceof CompoundIdentifierType.LongIdentifier) {
                                        return ((CompoundIdentifierType.LongIdentifier) longOrStringId).getValue();
                                    } else if (longOrStringId instanceof CompoundIdentifierType.StringIdentifier) {
                                        return ((CompoundIdentifierType.StringIdentifier) longOrStringId).getValue();
                                    } else {
                                        throw new RuntimeException("Unexpected type: " + longOrStringId.getClass().getName());
                                    }
                                })
                                .toArray()))
                .collect(Collectors.toList());
    }

    public static boolean isInsidePurgeRetention(final StatisticWrapper statisticWrapper,
                                                 final EventStoreTimeIntervalEnum interval,
                                                 final int retentionRowIntervals) {

        // round start time down to the last row key interval
        final long rowInterval = interval.rowKeyInterval();
        final long roundedNow = ((long) (System.currentTimeMillis() / rowInterval)) * rowInterval;
        final long cutOffTime = roundedNow - (rowInterval * retentionRowIntervals);

        return statisticWrapper.getTimeMs() >= cutOffTime;
    }

    private EventStoreTimeIntervalEnum computeInterval(final StatisticWrapper statisticWrapper) {

        //Start with the smallest precision defined in the stat config and iterate up from there
        Optional<EventStoreTimeIntervalEnum> currentInterval = Optional.of(
                EventStoreTimeIntervalEnum.fromColumnInterval(statisticWrapper.getOptionalStatisticConfiguration()
                        .orElseThrow(() -> new RuntimeException("Statistic configuration should never be null here as it has already been through validation"))
                        .getPrecision())
        );

        do {
            //TODO probably ought to cache this to save computing it each time
            //i.e. a cache of ESTIE:Integer with a short retention, e.g. a few mins
            String purgeRetentionPeriodsPropertyKey = HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                    + currentInterval.get().name().toLowerCase();
            final int retentionRowIntervals = stroomPropertyService.getIntPropertyOrThrow(purgeRetentionPeriodsPropertyKey);

            if (isInsidePurgeRetention(statisticWrapper, currentInterval.get(), retentionRowIntervals)) {
                //stat is fine to go into this interval, so drop out
                break;
            }
            //current interval no good so try the next biggest
            currentInterval = EventStoreTimeIntervalHelper.getNextBiggest(currentInterval.get());

        } while (currentInterval.isPresent());

        //if the interval is not present it means we were inside the purge retention earlier in the processing and
        //have just fallen out of it.  This is very unlikely to happen, so just assign the largest interval and
        //let it go through knowing it will get purged shortly.
        return currentInterval.orElseGet(() -> EventStoreTimeIntervalHelper.getLargestInterval());
    }

    protected KeyValue<StatKey, StatAggregate> buildKeyValue(final String statName,
                                                             final StatisticWrapper statisticWrapper,
                                                             final StatAggregate statAggregate) {

        //The stat will have been validated by this point so the get will succeed
        StatisticConfiguration statisticConfiguration = statisticWrapper.getOptionalStatisticConfiguration().get();
        Statistics.Statistic statistic = statisticWrapper.getStatistic();

        UID statNameUid = uniqueIdCache.getOrCreateId(statName);

        //convert rollupmask
        RollUpBitMask rollupMask = RollUpBitMask.ZERO_MASK;

        //convert interval - store it as ms in Long form
        EventStoreTimeIntervalEnum interval = computeInterval(statisticWrapper);

        //convert time to 8 byte Long ms since epoch
        //The time is left in its raw form and NOT truncated to its intended interval as that will happen
        //during the aggregation stage
        long timeMs = statisticWrapper.getTimeMs();

        //Get the list of tag names (in alphanumeric order) from the stat config,
        //then find the corresponding value from the stat event.
        //This way we allow for the stat event not including null values
        //We map null values to a magic null string
        List<TagType> tagTypes = statistic.getTags().getTag();
        //TODO may want to build a cache of statconf to a tuple3 of (statNameUID, List<TagValues>, ESTIE)
        //to speed the construction of the StatKey, though would still need to spawn a new list for the tagValues
        List<TagValue> tagValues = statisticConfiguration.getFieldNames().stream()
                .map(tag -> {
                    Optional<String> optValue = tagTypes.stream()
                            .filter(tagType -> tagType.getName().equals(tag))
                            .findFirst()
                            .map(TagType::getValue);

                    return buildTagValue(tag, optValue);
                })
                .collect(Collectors.toList());

        StatKey statKey = new StatKey(statNameUid, rollupMask, interval, timeMs, tagValues);

        return new KeyValue<>(statKey, statAggregate);
    }

    /**
     * FlatMap a single stat event into 1-* {@link KeyValue}(s) by generating all configured rollup permutations, e.g.
     * statName: MyStat tags|values: System|systemX,Environment|OPS
     * becomes
     * statName: MyStat tags: System|systemX,Environment|OPS
     * statName: MyStat tags: System|systemX,Environment|*
     * statName: MyStat tags: System|*,Environment|OPS
     * statName: MyStat tags: System|*,Environment|*
     * <p>
     * Assuming those rollup perms were configured in the StatisticConfiguration object
     * <p>
     * This flat mapping relies on having access to the statistic configuration for this statName
     */
    protected List<KeyValue<StatKey, StatAggregate>> buildKeyValues(final String statName,
                                                                    final StatisticWrapper statisticWrapper,
                                                                    final StatAggregate statAggregate) {

        KeyValue<StatKey, StatAggregate> baseKeyValue = buildKeyValue(statName, statisticWrapper, statAggregate);

        Statistics.Statistic statistic = statisticWrapper.getStatistic();
        StatisticConfiguration statisticConfiguration = statisticWrapper.getOptionalStatisticConfiguration()
                .orElseThrow(() -> new RuntimeException("Statistic configuration should never be null here as it has already been through validation"));

        final int tagListSize = statistic.getTags().getTag().size();
        final StatisticRollUpType rollUpType = statisticConfiguration.getRollUpType();


        if (tagListSize == 0 || StatisticRollUpType.NONE.equals(rollUpType)) {

            //no tags or roll up perms configured so just return the base key value
            return Collections.singletonList(baseKeyValue);

        } else {
            final Set<RollUpBitMask> rollUpBitMasks;
            switch (rollUpType) {
                case CUSTOM:
                    rollUpBitMasks = statisticConfiguration.getCustomRollUpMasks().stream()
                            .map(customMask ->
                                    RollUpBitMask.fromTagPositions(customMask.getRolledUpTagPositions()))
                            .collect(Collectors.toSet());
                    break;
                case ALL:
                    rollUpBitMasks = RollUpBitMask.getRollUpBitMasks(tagListSize);
                    break;
                default:
                    throw new RuntimeException("Should never get here");
            }

            LOGGER.trace(() -> String.format("Using rollUpBitMasks %s", rollUpBitMasks));

            //use each of the rollUpBitMasks to spawn a new statkey (apart from the zero mask that we already
            //have in the baseKeyValue) and from that make a new KeyValue with the same statAggregate value
            //Each spawned KeyValue should mostly be references back to the base one so the memory and processing
            //cost should be lower than using deep copies
            List<KeyValue<StatKey, StatAggregate>> keyValuePerms = rollUpBitMasks.stream()
                    .filter(rollUpBitMask -> !rollUpBitMask.equals(RollUpBitMask.ZERO_MASK))
                    .map(rollUpBitMask -> {
                        StatKey newStatKey = baseKeyValue.key.cloneAndRollUpTags(rollUpBitMask, rolledUpValue);
                        return new KeyValue<>(newStatKey, statAggregate);
                    })
                    .collect(Collectors.toList());

            keyValuePerms.add(baseKeyValue);
            LOGGER.trace(() -> String.format("Returning %s keyValues", keyValuePerms.size()));
            return keyValuePerms;
        }
    }

}
