

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

package stroom.stats.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import stroom.stats.api.StatisticTag;
import stroom.stats.hbase.structure.RowKeyTagValue;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.hbase.uid.UniqueIdFetchMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Static methods only
 */
public class TagValueConverter {
    private TagValueConverter() {
        // Static methods only should never be created
    }

    public static List<RowKeyTagValue> convert(final List<StatisticTag> statisticTags,
            final UniqueIdCache uniqueIdCache, final UniqueIdFetchMode uniqueIdFetchMode) {
        final List<RowKeyTagValue> tagValuePairs = new ArrayList<>();

        // loop through all the objects in the event and convert them into
        // tagValue pairs
        for (final StatisticTag statisticTag : statisticTags) {
            tagValuePairs.add(convert(statisticTag, uniqueIdCache, uniqueIdFetchMode));
        }

        // we need to sort the tagValue pairs in their byte form so that they
        // are always in the same order in the row key
        // this means we can make optimisations when searching the data
        // This is still needed despite the tags being sorted in the
        // StatisticEvent as they must be ordered by the UID
        // bytes.
        tagValuePairs.sort((pair1, pair2) -> Bytes.compareTo(pair1.asByteArray(), pair2.asByteArray()));

        return tagValuePairs;
    }

    public static RowKeyTagValue convert(final StatisticTag statisticTag, final UniqueIdCache uniqueIdCache,
            final UniqueIdFetchMode uniqueIdFetchMode) {
        final String value;

        if (statisticTag.getValue() == null)
            value = StatisticTag.NULL_VALUE_STRING;
        else
            value = statisticTag.getValue();

        // Get uid for obj type.
        final UID tagUid = uniqueIdCache.getCreateOrDefaultId(statisticTag.getTag(), uniqueIdFetchMode);
        // Get uid for obj name.
        final UID valueUid = uniqueIdCache.getCreateOrDefaultId(value, uniqueIdFetchMode);
        return new RowKeyTagValue(tagUid, valueUid);
    }

    public static StatisticTag convert(final RowKeyTagValue rowKeyTagValue, final UniqueIdCache uniqueIdCache) {
        final String tag = getTag(rowKeyTagValue, uniqueIdCache);
        final String value = getValue(rowKeyTagValue, uniqueIdCache);
        return new StatisticTag(tag, value);
    }

    public static Map<String, String> getTagValuePairsAsMap(final List<RowKeyTagValue> tagValuePairs,
            final UniqueIdCache uniqueIdCache) {
        final Map<String, String> map = new TreeMap<>();

        tagValuePairs.forEach((rowKeyTagValue) -> {
            final String tag = getTag(rowKeyTagValue, uniqueIdCache);
            final String value = getValue(rowKeyTagValue, uniqueIdCache);
            map.put(tag, value);
        });
        return map;
    }

    public static List<StatisticTag> getTagValuePairsAsList(final List<RowKeyTagValue> tagValuePairs,
            final UniqueIdCache uniqueIdCache) {
        return tagValuePairs.stream().map((rowKeyTagValue) -> convert(rowKeyTagValue, uniqueIdCache))
                .collect(Collectors.toList());
    }

    public static String getTag(final RowKeyTagValue rowKeyTagValue, final UniqueIdCache uniqueIdCache) {
        return uniqueIdCache.getName(rowKeyTagValue.getTag());
    }

    /**
     * @return The value or null if the UID mapped to the magic null value
     */
    public static String getValue(final RowKeyTagValue rowKeyTagValue, final UniqueIdCache uniqueIdCache) {
        final String value = uniqueIdCache.getName(rowKeyTagValue.getValue());
        return StatisticTag.NULL_VALUE_STRING.equals(value) ? null : value;
    }
}
