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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.uid.UniqueIdCache;

import java.util.stream.Collectors;

public class StatKeyUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatKeyUtils.class);

    public static void logStatKey(final UniqueIdCache uniqueIdCache, final StatKey statKey) {

        String statName = getStatName(uniqueIdCache, statKey);
        String tagValues = getTagValues(uniqueIdCache, statKey, ",", "|");

        LOGGER.debug("StatKey - {}, {}, {}, {}", statName, statKey.getRollupMask(), statKey.getInterval(), tagValues);
    }

    public static String getStatName(final UniqueIdCache uniqueIdCache, final StatKey statKey) {
        return uniqueIdCache.getName(statKey.getStatName());
    }

    public static String getTagValues(final UniqueIdCache uniqueIdCache,
                                      final StatKey statKey,
                                      final String tagValueDelimiter,
                                      final String pairDelimiter) {

        return statKey.getTagValues().stream()
                .map(tagValue ->
                        uniqueIdCache.getName(tagValue.getTag()) + pairDelimiter +
                                uniqueIdCache.getName(tagValue.getValue()
                                ))
                .collect(Collectors.joining(tagValueDelimiter));
    }
}
