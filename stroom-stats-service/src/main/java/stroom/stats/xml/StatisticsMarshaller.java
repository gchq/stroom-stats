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

package stroom.stats.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.schema.v3.Statistics;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;

public class StatisticsMarshaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsMarshaller.class);

    private final JAXBContext jaxbContext;
    private int countOfFailedDeserialisations = 0;

    public StatisticsMarshaller() {
        try {
            this.jaxbContext = JAXBContext.newInstance(Statistics.class);
        } catch (JAXBException e) {
            throw new RuntimeException(String.format("Error creating new JAXBContext instance for %s", Statistics.class.getName()), e);
        }
    }

    public Statistics unMarshallXml(String xmlStr) {
        try {
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            Statistics statistics =  (Statistics) unmarshaller.unmarshal(new StringReader(xmlStr));
            if (LOGGER.isTraceEnabled()) {
                logStatistics(statistics);
            }
            return statistics;
        } catch (JAXBException e) {
            int trimIndex = xmlStr.length() < 50 ? xmlStr.length() : 49;
            LOGGER.error("Unable to deserialise a message (enable debug to log full message): {}...", xmlStr.substring(0, trimIndex));
            LOGGER.debug("Unable to deserialise a message {}", xmlStr);
            countOfFailedDeserialisations++;
            LOGGER.error("Error un-marshalling message value");
            throw new RuntimeException(String.format("Error un-marshalling message value"), e);
        }
    }

    public String marshallXml(Statistics statistics) {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            StringWriter stringWriter = new StringWriter();
            marshaller.marshal(statistics, stringWriter);
            return stringWriter.toString();
        } catch (JAXBException e) {
            LOGGER.error("Error marshalling message value");
            throw new RuntimeException(String.format("Error marshalling message value"), e);
        }
    }

    private void logStatistics(Statistics statistics) {
        statistics.getStatistic().forEach(statistic ->
                LOGGER.trace("Un-marshalling stat with name {}, count {} and value {}", statistic.getName(), statistic.getCount(), statistic.getValue())
        );
    }

}
