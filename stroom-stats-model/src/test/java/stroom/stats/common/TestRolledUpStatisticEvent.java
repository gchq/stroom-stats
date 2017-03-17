

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

import org.junit.Test;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.StatisticType;
import stroom.stats.api.TimeAgnosticStatisticEvent;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRolledUpStatisticEvent {
    private static final double JUNIT_DOUBLE_EQUALITY_DELTA = 0.001;

    private final List<StatisticTag> tags = new ArrayList<StatisticTag>();
    private StatisticEvent event;

    @Test
    public void testRolledUpStatisticEventStatisticEventListOfListOfStatisticTag() {
        buildEvent();
    }

    @Test
    public void testRolledUpStatisticEventStatisticEvent() {
        buildEvent();

        final RolledUpStatisticEvent rolledUpStatisticEvent = new RolledUpStatisticEvent(event);

        compareEvents(event, rolledUpStatisticEvent);

        assertTrue(rolledUpStatisticEvent.iterator().hasNext());

        int counter = 0;
        for (final TimeAgnosticStatisticEvent timeAgnosticStatisticEvent : rolledUpStatisticEvent) {
            assertEquals(event.getTimeAgnosticStatisticEvent(), timeAgnosticStatisticEvent);

            counter++;
        }

        assertEquals(1, counter);
    }

    private void buildEvent() {
        tags.add(new StatisticTag("Tag1", "Tag1Val"));
        tags.add(new StatisticTag("Tag2", "Tag2Val"));
        tags.add(new StatisticTag("Tag3", "Tag3Val"));

        event = new StatisticEvent(1234L, "MtStat", tags, 1000L);
    }

    private void compareEvents(final StatisticEvent statisticEvent,
            final RolledUpStatisticEvent rolledUpStatisticEvent) {
        assertEquals(statisticEvent.getTimeMs(), rolledUpStatisticEvent.getTimeMs());
        assertEquals(statisticEvent.getName(), rolledUpStatisticEvent.getName());
        assertEquals(statisticEvent.getType(), rolledUpStatisticEvent.getType());

        if (statisticEvent.getType().equals(StatisticType.COUNT)) {
            assertEquals(statisticEvent.getCount(), rolledUpStatisticEvent.getCount());
        } else {
            assertEquals(statisticEvent.getValue(), rolledUpStatisticEvent.getValue(), JUNIT_DOUBLE_EQUALITY_DELTA);
        }
    }
}
