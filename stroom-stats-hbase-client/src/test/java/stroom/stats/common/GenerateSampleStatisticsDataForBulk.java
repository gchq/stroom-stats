

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
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class GenerateSampleStatisticsDataForBulk {
    private static final String START_DATE = "2015-05-01T00:00:00.000Z";

    // 52,000 is just over 3 days at 5000ms intervals
    private static final int ITERATION_COUNT = 200_000;
    private static final int EVENT_TIME_DELTA_MS = 5000;

    private static final String COLOUR_RED = "Red";
    private static final String COLOUR_GREEN = "Green";
    private static final String COLOUR_BLUE = "Blue";

    private static final Random RANDOM = new Random(293874928374298744L);

    private static final List<String> COLOURS = Arrays.asList(COLOUR_RED, COLOUR_GREEN, COLOUR_BLUE);

    private static final List<String> STATES = Arrays.asList("IN", "OUT");

    private static final String[] users = new String[] { "user1", "user2", "user3", "user4", "user5", };

    public static void main(final String[] args) throws Exception {
        System.out.println("Writing value data...");

        Writer writer;

        writer = new FileWriter(new File("StatsBulkTestData_Values.xml"));

        generateValueData(writer);
        writer.close();

        System.out.println("Writing count data...");

        writer = new FileWriter(new File("StatsBulkTestData_Counts.xml"));

        generateCountData(writer);

        writer.close();

        System.out.println("Finished!");

    }

    public static String generateValueData(final Writer writer) throws IOException {
        final long eventTime = DateUtil.parseNormalDateTimeString(START_DATE);

        final StringBuilder stringBuilder = new StringBuilder();

        buildEvents(writer, eventTime, StatisticType.VALUE);

        return stringBuilder.toString();
    }

    public static String generateCountData(final Writer writer) throws IOException {
        final long eventTime = DateUtil.parseNormalDateTimeString(START_DATE);

        final StringBuilder stringBuilder = new StringBuilder();

        buildEvents(writer, eventTime, StatisticType.COUNT);

        return stringBuilder.toString();
    }

    private static void buildEvents(final Writer writer, final long initialEventTime, final StatisticType statisticType)
            throws IOException {
        long eventTime = initialEventTime;

        writer.write("<data>\n");

        final StringBuilder stringBuilder = new StringBuilder();

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
                            stringBuilder.append("<value>" + (RANDOM.nextInt(10) + 1) + "</value>");
                        } else {
                            final String val = Double.toString(RANDOM.nextInt(100) + RANDOM.nextDouble());
                            stringBuilder.append("<value>" + val + "</value>");
                        }
                        stringBuilder.append("</event>\n");

                        writer.write(stringBuilder.toString());
                        stringBuilder.setLength(0);
                    }
                }
            }
            eventTime += EVENT_TIME_DELTA_MS;
        }
        writer.write("</data>\n");
    }
}
