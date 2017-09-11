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

package stroom.stats.test;

import stroom.stats.schema.v3.CompoundIdentifierType;
import stroom.stats.schema.v3.ObjectFactory;
import stroom.stats.schema.v3.Statistics;
import stroom.stats.schema.v3.TagType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsHelper {

    //hold a map of uuids for each stat name to make it easier for test classes to reuse uuids
    private static final Map<String, String> NAME_TO_UUID_MAP = new ConcurrentHashMap<>();

    public static Statistics.Statistic buildCountStatistic(String statName,
                                                           ZonedDateTime time,
                                                           long value,
                                                           TagType... tagValues) {
        return buildCountStatistic(getUuidKey(statName), statName, time, value, tagValues);
    }

    public static Statistics.Statistic buildCountStatistic(String key,
                                                           String statName,
                                                           ZonedDateTime time,
                                                           long value,
                                                           TagType... tagValues) {
        Statistics.Statistic statistic = buildStatistic(key, statName, time, tagValues);
        statistic.setCount(value);
        return statistic;
    }

    public static Statistics.Statistic buildValueStatistic(String statName,
                                                           ZonedDateTime time,
                                                           double value,
                                                           TagType... tagValues) {
        return buildValueStatistic(getUuidKey(statName), statName, time, value, tagValues);
    }

    public static Statistics.Statistic buildValueStatistic(String key, String statName, ZonedDateTime time, double value, TagType... tagValues) {
        Statistics.Statistic statistic = buildStatistic(key, statName, time, tagValues);
        statistic.setValue(value);
        return statistic;
    }

    public static void addIdentifier(final Statistics.Statistic statistic, long... identifierParts) {

        CompoundIdentifierType id = new CompoundIdentifierType();
        int i = 1;
        for (long idPartValue : identifierParts) {
            CompoundIdentifierType.LongIdentifier idpart = new CompoundIdentifierType.LongIdentifier();
            idpart.setName("Part" + i);
            idpart.setValue(idPartValue);
            id.getLongIdentifierOrStringIdentifier().add(idpart);
        }
        Statistics.Statistic.Identifiers identifiers = statistic.getIdentifiers();
        if (identifiers == null) {
            identifiers = new Statistics.Statistic.Identifiers();
            statistic.setIdentifiers(identifiers);
        }
        identifiers.getCompoundIdentifier().add(id);

    }

    public static Statistics buildStatistics(Statistics.Statistic... statisticObjects) {
        return buildStatistics(Arrays.asList(statisticObjects));
    }

    public static Statistics buildStatistics(List<Statistics.Statistic> statisticList) {
        Statistics statistics = new ObjectFactory().createStatistics();
        statistics.getStatistic().addAll(statisticList);
        return statistics;
    }

    public static TagType buildTagType(String tag, String value) {
        TagType tagType = new TagType();
        tagType.setName(tag);
        tagType.setValue(value);
        return tagType;
    }

    private static Statistics.Statistic buildStatistic(String key, String statName, ZonedDateTime time, TagType... tagValues) {
        ObjectFactory objectFactory = new ObjectFactory();
        Statistics.Statistic statistic = objectFactory.createStatisticsStatistic();
        Statistics.Statistic.Key keyObj = objectFactory.createStatisticsStatisticKey();
        keyObj.setValue(key);
        keyObj.setStatisticName(statName);
        statistic.setKey(keyObj);
        Statistics.Statistic.Tags tagsObj = new Statistics.Statistic.Tags();
        tagsObj.getTag().addAll(new ArrayList(Arrays.asList(tagValues)));
        statistic.setTags(tagsObj);
        GregorianCalendar gregorianCalendar = GregorianCalendar.from(time);
        try {
            statistic.setTime(DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar));
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException(String.format("Error converting time %s to a gregorian calendar", time), e);
        }
        return statistic;
    }

    public static String getUuidKey(final String statName) {
        NAME_TO_UUID_MAP.computeIfAbsent(statName, k -> UUID.randomUUID().toString());
        return NAME_TO_UUID_MAP.get(statName);
    }
}
