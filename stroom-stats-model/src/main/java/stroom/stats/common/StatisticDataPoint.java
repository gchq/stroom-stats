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

package stroom.stats.common;

import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.List;
import java.util.Map;

public interface StatisticDataPoint {

    String getStatisticName();

    long getTimeMs();

    long getPrecisionMs();

    default String getPrecision() {
        return EventStoreTimeIntervalEnum.fromColumnInterval(getPrecisionMs()).longName();
    }

    List<StatisticTag> getTags();

    Map<String, String> getTagsAsMap();

    StatisticType getStatisticType();

//    Map<String, Object> getFieldToValueMap();

    String getFieldValue(final String fieldName);

}
