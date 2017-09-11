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

package stroom.stats.schema;

import org.junit.Test;
import stroom.stats.schema.v3.Statistics;
import stroom.stats.xml.StatisticsMarshaller;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SerialisationTest {
    private final String RESOURCES_DIR = "src/test/resources";
    private final String PACKAGE_NAME = "/stroom/stats/schema/";
    private final String EXAMPLE_XML_01 = RESOURCES_DIR + PACKAGE_NAME + "SerialisationTest_testDererialisation.xml";
    private final String STATISTICS_FROM_STROOM_01 = RESOURCES_DIR + PACKAGE_NAME + "StatisticsFromStroom_01.xml";
    private final String STATISTICS_FROM_STROOM_02 = RESOURCES_DIR + PACKAGE_NAME + "StatisticsFromStroom_02.xml";

    private final StatisticsMarshaller statisticsMarshaller;

    public SerialisationTest() {
        this.statisticsMarshaller = new StatisticsMarshaller();
    }

    @Test
    public void testDeserialisation() throws IOException, JAXBException {

        String xmlStr = new String(Files.readAllBytes(Paths.get(EXAMPLE_XML_01)), StandardCharsets.UTF_8);
        Statistics statistics = statisticsMarshaller.unMarshallFromXml(xmlStr);

        // Check the number of stats is right
        assertThat(statistics.getStatistic().size(), equalTo(3));

        // Check some other things
        assertThat(statistics.getStatistic().get(0).getCount(), equalTo(2l));
        assertThat(statistics.getStatistic().get(1).getCount(), equalTo(1l));
        assertThat(statistics.getStatistic().get(2).getCount(), equalTo(null));
        assertThat(statistics.getStatistic().get(2).getValue(), equalTo(99.9));
        assertThat(statistics.getStatistic().get(0).getTags().getTag().get(0).getName(), equalTo("department"));
        assertThat(statistics.getStatistic().get(0).getTags().getTag().get(1).getValue(), equalTo("jbloggs"));
    }


    @Test
    public void testPostStatisticsFromStroom_01() throws JAXBException, IOException {
        String fileString = new String(Files.readAllBytes(Paths.get(STATISTICS_FROM_STROOM_01)));
        Statistics statistics = statisticsMarshaller.unMarshallFromXml(fileString);
        assertThat(statistics.getStatistic().size(), equalTo(99));
    }

    @Test
    public void testPostStatisticsFromStroom_02() throws JAXBException, IOException {
        String fileString = new String(Files.readAllBytes(Paths.get(STATISTICS_FROM_STROOM_02)));
        Statistics statistics = statisticsMarshaller.unMarshallFromXml(fileString);
        assertThat(statistics.getStatistic().size(), equalTo(99));
    }
}
