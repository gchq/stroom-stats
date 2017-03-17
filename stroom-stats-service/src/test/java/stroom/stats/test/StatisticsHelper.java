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

import stroom.stats.schema.CompoundIdentifierType;
import stroom.stats.schema.ObjectFactory;
import stroom.stats.schema.Statistics;
import stroom.stats.schema.TagType;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.GregorianCalendar;

public class StatisticsHelper {

    public static Statistics.Statistic buildCountStatistic(String statName, ZonedDateTime time, long value, TagType... tagValues) throws DatatypeConfigurationException {
        Statistics.Statistic statistic = new ObjectFactory().createStatisticsStatistic();
        statistic.setName(statName);
        Statistics.Statistic.Tags tagsObj = new Statistics.Statistic.Tags();
        for (TagType tagValue : tagValues) {
            tagsObj.getTag().add(tagValue);
        }
        statistic.setTags(tagsObj);
        GregorianCalendar gregorianCalendar = GregorianCalendar.from(time);
        statistic.setTime(DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar));
        statistic.setCount(value);
        return statistic;
    }

    public static Statistics.Statistic buildValueStatistic(String statName, ZonedDateTime time, double value, TagType... tagValues) throws DatatypeConfigurationException {
        Statistics.Statistic statistic = new ObjectFactory().createStatisticsStatistic();
        statistic.setName(statName);
        Statistics.Statistic.Tags tagsObj = new Statistics.Statistic.Tags();
        for (TagType tagValue : tagValues) {
            tagsObj.getTag().add(tagValue);
        }
        statistic.setTags(tagsObj);
        GregorianCalendar gregorianCalendar = GregorianCalendar.from(time);
        statistic.setTime(DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar));
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
        Statistics statistics = new ObjectFactory().createStatistics();
        statistics.getStatistic().addAll(Arrays.asList(statisticObjects));
        return statistics;
    }

    public static TagType buildTagType(String tag, String value) {
        TagType tagType = new TagType();
        tagType.setName(tag);
        tagType.setValue(value);
        return tagType;
    }
}
