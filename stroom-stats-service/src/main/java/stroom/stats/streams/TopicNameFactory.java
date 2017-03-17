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

import stroom.stats.api.StatisticType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

public final class TopicNameFactory {

    public static final String DELIMITER = "-";

    private TopicNameFactory() {
    }

    public static String getStatisticTypedName(final String prefix, final StatisticType statisticType) {
       return prefix + DELIMITER + statisticType.getDisplayValue();
    }

    public static String getIntervalTopicName(final String prefix, final EventStoreTimeIntervalEnum interval) {
        return prefix + DELIMITER + interval.shortName();
    }

    public static String getIntervalTopicName(final String prefix, final StatisticType statisticType, final EventStoreTimeIntervalEnum interval) {
        return getIntervalTopicName(getStatisticTypedName(prefix, statisticType), interval);
    }
}
