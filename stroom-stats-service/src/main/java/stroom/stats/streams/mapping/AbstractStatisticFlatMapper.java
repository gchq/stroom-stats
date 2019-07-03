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

import com.google.common.base.Preconditions;
import org.apache.kafka.streams.KeyValue;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.hbase.HBaseStatisticConstants;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.v4.CompoundIdentifierType;
import stroom.stats.schema.v4.Statistics;
import stroom.stats.schema.v4.TagType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractStatisticFlatMapper implements StatisticFlatMapper {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(AbstractStatisticFlatMapper.class);

    private final UniqueIdCache uniqueIdCache;
    private final StroomPropertyService stroomPropertyService;
    private final UID rolledUpValue;

//    private Map<StatEventKey, StatAggregate> putEventsMap = new HashMap<>();

    public AbstractStatisticFlatMapper(final UniqueIdCache uniqueIdCache,
                                       final StroomPropertyService stroomPropertyService) {
        this.uniqueIdCache = uniqueIdCache;
        this.stroomPropertyService = stroomPropertyService;

        //can hold this byte value for the life of the application as it will nto change
        rolledUpValue = uniqueIdCache.getOrCreateId(RollUpBitMask.ROLL_UP_TAG_VALUE);
    }


    private TagValue buildTagValue(String tag, Optional<String> value) {

        Preconditions.checkNotNull(tag);

        LOGGER.trace(() -> String.format("Creating TagValue tag: %s value %s", tag, value.orElse("NULL")));
        UID tagUid = uniqueIdCache.getOrCreateId(tag);
        UID valueUid = uniqueIdCache.getOrCreateId(value.orElse(NULL_VALUE_STRING));
        return new TagValue(tagUid, valueUid);
    }

    protected List<MultiPartIdentifier> convertEventIds(final Statistics.Statistic statistic, final int maxEventIds) {

        if (statistic.getIdentifiers() == null || statistic.getIdentifiers().getCompoundIdentifier() == null) {
            return Collections.emptyList();
        }
        try {
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
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error converting eventIds for stat %s", statistic), e);
        }
    }

    private EventStoreTimeIntervalEnum computeInterval(final StatisticWrapper statisticWrapper) {

        //Start with the smallest precision defined in the stat config and iterate up from there
        Optional<EventStoreTimeIntervalEnum> currentInterval = Optional.of(
                statisticWrapper.getOptionalStatisticConfiguration()
                        .orElseThrow(() -> new RuntimeException("Statistic configuration should never be null here as it has already been through validation"))
                        .getPrecision()
        );

        do {
            //TODO probably ought to cache this to save computing it each time
            //i.e. a cache of ESTIE:Integer with a short retention, e.g. a few mins
            String purgeRetentionPeriodsPropertyKey = HBaseStatisticConstants.DATA_STORE_PURGE_INTERVALS_TO_RETAIN_PROPERTY_NAME_PREFIX
                    + currentInterval.get().name().toLowerCase();
            final int retentionRowIntervals = stroomPropertyService.getIntPropertyOrThrow(purgeRetentionPeriodsPropertyKey);

            if (StatisticFlatMapper.isInsidePurgeRetention(statisticWrapper, currentInterval.get(), retentionRowIntervals)) {
                //stat is fine to go into this interval, so drop out
                break;
            }
            //current interval no good so try the next biggest
            currentInterval = EventStoreTimeIntervalEnum.getNextBiggest(currentInterval.get());

        } while (currentInterval.isPresent());

        //if the interval is not present it means we were inside the purge retention earlier in the processing and
        //have just fallen out of it.  This is very unlikely to happen, so just assign the largest interval and
        //let it go through knowing it will get purged shortly.
        return currentInterval.orElseGet(() -> EventStoreTimeIntervalEnum.getLargestInterval());
    }

    protected KeyValue<StatEventKey, StatAggregate> buildKeyValue(final String statUuid,
                                                                  final StatisticWrapper statisticWrapper,
                                                                  final StatAggregate statAggregate) {

        //The stat will have been validated by this point so the get will succeed
        StatisticConfiguration statisticConfiguration = statisticWrapper.getOptionalStatisticConfiguration().get();
        Statistics.Statistic statistic = statisticWrapper.getStatistic();

        UID statUuidUid = uniqueIdCache.getOrCreateId(statUuid);

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
        List<TagValue> tagValues;
        if (statistic.getTags() != null) {
            List<TagType> tagTypes = statistic.getTags().getTag();
            //TODO may want to build a cache of statconf to a tuple3 of (statNameUID, List<TagValues>, ESTIE)
            //to speed the construction of the StatEventKey, though would still need to spawn a new list for the tagValues
            tagValues = statisticConfiguration.getFieldNames().stream()
                    .map(tag -> {
                        Optional<String> optValue = tagTypes.stream()
                                .filter(tagType -> tagType.getName().equals(tag))
                                .findFirst()
                                .map(TagType::getValue);

                        return buildTagValue(tag, optValue);
                    })
                    .collect(Collectors.toList());
        } else {
            tagValues = Collections.emptyList();
        }

        StatEventKey statEventKey = new StatEventKey(statUuidUid, rollupMask, interval, timeMs, tagValues);

        return new KeyValue<>(statEventKey, statAggregate);
    }

    /**
     * FlatMap a single stat event into 1-* {@link KeyValue}(s) by generating all configured rollup permutations, e.g.
     * statUuid: 180b843a-... tags|values: System|systemX,Environment|OPS
     * becomes
     * statUuid: 180b843a-... tags: System|systemX,Environment|OPS
     * statUuid: 180b843a-... tags: System|systemX,Environment|*
     * statUuid: 180b843a-... tags: System|*,Environment|OPS
     * statUuid: 180b843a-... tags: System|*,Environment|*
     * <p>
     * Assuming those rollup perms were configured in the StatisticConfiguration object
     * <p>
     * This flat mapping relies on having access to the statistic configuration for this statName
     */
    protected List<KeyValue<StatEventKey, StatAggregate>> buildKeyValues(final String statUuid,
                                                                         final StatisticWrapper statisticWrapper,
                                                                         final StatAggregate statAggregate) {

        KeyValue<StatEventKey, StatAggregate> baseKeyValue = buildKeyValue(statUuid, statisticWrapper, statAggregate);

        Statistics.Statistic statistic = statisticWrapper.getStatistic();
        StatisticConfiguration statisticConfiguration = statisticWrapper.getOptionalStatisticConfiguration()
                .orElseThrow(() -> new RuntimeException("Statistic configuration should never be null here as it has already been through validation"));

        //bad jaxb naming
        final int tagListSize = statistic.getTags() == null ? 0 : statistic.getTags().getTag().size();
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
            List<KeyValue<StatEventKey, StatAggregate>> keyValuePerms = rollUpBitMasks.stream()
                    .filter(rollUpBitMask -> !rollUpBitMask.equals(RollUpBitMask.ZERO_MASK))
                    .map(rollUpBitMask -> {
                        StatEventKey newStatEventKey = baseKeyValue.key.cloneAndRollUpTags(rollUpBitMask, rolledUpValue);
                        return new KeyValue<>(newStatEventKey, statAggregate);
                    })
                    .collect(Collectors.toList());

            keyValuePerms.add(baseKeyValue);
            LOGGER.trace(() -> String.format("Returning %s keyValues", keyValuePerms.size()));
//            LOGGER.ifDebugIsEnabled(() -> {
//
//                keyValuePerms.forEach(kv -> {
//                    putEventsMap.computeIfPresent(kv.key, (k, v) -> {
//                        if (kv.key.getRollupMask().equals(RollUpBitMask.ZERO_MASK) &&
//                                kv.key.getInterval().equals(EventStoreTimeIntervalEnum.SECOND)) {
//
//                            LOGGER.debug("Existing key {}", k.toString());
//                            LOGGER.debug("New      key {}", kv.key.toString());
//                            LOGGER.debug("Seen duplicate key");
//                        }
//                        return v.aggregate(kv.value, 100);
//                    });
//                    putEventsMap.put(kv.key, kv.value);
//                });
//            });
            return keyValuePerms;
        }
    }
}
