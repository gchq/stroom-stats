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

package stroom.stats.adapters;

import com.google.common.base.Preconditions;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.schema.Statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StatisticEventAdapter {
    private StatisticEventAdapter() {
    }

    public static StatisticEvent convert(Statistics.Statistic sourceStat) {
        Preconditions.checkNotNull(sourceStat);

        final List<StatisticTag> destTags;
        if (sourceStat.getTags() != null) {
            destTags = sourceStat.getTags().getTag().stream()
                    .map(sourceTag -> new StatisticTag(sourceTag.getName(), sourceTag.getValue()))
                    .collect(Collectors.toList());
        } else {
            destTags = Collections.emptyList();
        }

        final List<MultiPartIdentifier> identifiers = new ArrayList<>();
        //TODO handle compound identifiers

        long timeMs = sourceStat.getTime().toGregorianCalendar().getTimeInMillis();

        if (sourceStat.getCount() != null) {
            return new StatisticEvent(timeMs, sourceStat.getName(), destTags, identifiers, sourceStat.getCount());
        } else if (sourceStat.getValue() != null) {
            return new StatisticEvent(timeMs, sourceStat.getName(), destTags, identifiers, sourceStat.getValue());
        } else {
            throw new RuntimeException("No count or value");
        }
    }
}
