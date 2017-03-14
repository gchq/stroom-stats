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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.uid.UniqueIdCache;

import java.util.stream.Collectors;

public class StatKeyUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatKeyUtils.class);

    public static void logStatKey(final UniqueIdCache uniqueIdCache, final StatKey statKey) {

        String statName = uniqueIdCache.getName(statKey.getStatName());
        String tagValues = statKey.getTagValues().stream()
                .map(tagValue ->
                        uniqueIdCache.getName(tagValue.getTag()) + "|" +
                                uniqueIdCache.getName(tagValue.getValue()
                                ))
                .collect(Collectors.joining(","));
        LOGGER.info("StatKey - {}, {}, {}, {}", statName, statKey.getRollupMask(), statKey.getInterval(), tagValues);
    }
}
