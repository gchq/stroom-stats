

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

import stroom.stats.api.StatisticType;
import stroom.stats.util.DateUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;


public class GenerateSampleStatisticsDataForPurge {
    private static final String START_DATE = "2014-01-01T00:00:00.000Z";

    private static final String USER1 = "user1";
    private static final String USER2 = "user2";

    // at 45min intervals this should be just over 2 years
    private static final int ITERATION_COUNT = 24_000;

    // generate events every 45mins
    private static final int EVENT_TIME_DELTA_MS = 45 * 60 * 1000;

    private static final String COLOUR_RED = "Red";
    private static final String COLOUR_GREEN = "Green";
    private static final String COLOUR_BLUE = "Blue";

    private static final List<String> COLOURS = Arrays.asList(COLOUR_RED, COLOUR_GREEN, COLOUR_BLUE);

    private static final List<String> STATES = Arrays.asList("IN", "OUT");

    private static final String[] users = new String[] { USER1, USER2 };

    public static void main(final String[] args) throws Exception {
        System.out.println("Writing value data...");

        Writer writer = new FileWriter(new File("StatsPurgeTestData_Values.xml"));

        writer.write(generateValueData());
        writer.close();

        System.out.println("Writing count data...");

        writer = new FileWriter(new File("StatsPurgeTestData_Counts.xml"));

        writer.write(generateCountData());

        writer.close();

        System.out.println("Finished!");

    }

    private static long generateStartTime() {
        final int hourInMs = 1000 * 60 * 60;

        long startTime = System.currentTimeMillis();

        // round the star time to the last whole hour
        startTime = new Long(startTime / hourInMs) * hourInMs;

        // now add 1min and 1s to help us see which store it is in
        startTime += 61_000;

        return startTime;
    }

    public static String generateValueData() {
        final long eventTime = generateStartTime();

        final StringBuilder stringBuilder = new StringBuilder();

        buildEvents(stringBuilder, eventTime, StatisticType.VALUE);

        return stringBuilder.toString();
    }

    public static String generateCountData() {
        final long eventTime = generateStartTime();

        final StringBuilder stringBuilder = new StringBuilder();

        buildEvents(stringBuilder, eventTime, StatisticType.COUNT);

        return stringBuilder.toString();
    }

    private static void buildEvents(final StringBuilder stringBuilder, final long initialEventTime,
            final StatisticType statisticType) {
        long eventTime = initialEventTime;

        stringBuilder.append("<data>\n");

        for (int i = 0; i <= ITERATION_COUNT; i++) {
            for (final String user : users) {
                for (final String colour : COLOURS) {
                    for (final String state : STATES) {
                        stringBuilder.append("<event>");
                        stringBuilder.append("<time>" + DateUtil.createNormalDateTimeString(eventTime) + "</time>");
                        stringBuilder.append("<user>" + user + "</user>");
                        stringBuilder.append("<colour>" + colour + "</colour>");
                        stringBuilder.append("<state>" + state + "</state>");

                        if (statisticType.equals(StatisticType.COUNT)) {
                            stringBuilder.append("<value>" + 1 + "</value>");
                        } else {
                            String val = "";
                            if (colour.equals(COLOUR_RED)) {
                                val = "10.1";
                            } else if (colour.equals(COLOUR_GREEN)) {
                                val = "20.2";
                            } else if (colour.equals(COLOUR_BLUE)) {
                                val = "69.7";
                            }
                            stringBuilder.append("<value>" + val + "</value>");
                        }
                        stringBuilder.append("</event>\n");
                    }
                }
            }
            // working backwards from the start date
            eventTime -= EVENT_TIME_DELTA_MS;
        }
        stringBuilder.append("</data>\n");
    }
}
