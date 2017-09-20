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

package stroom.stats.schema.v4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to hold the JAXB Context for (un-)marshalling {@link Statistics} xml/objects
 */
@Singleton
public class StatisticsMarshaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsMarshaller.class);

    private final JAXBContext jaxbContext;

    public StatisticsMarshaller() {
        try {
            this.jaxbContext = JAXBContext.newInstance(Statistics.class);
        } catch (JAXBException e) {
            throw new RuntimeException(String.format(
                    "Error creating new JAXBContext instance for %s",
                    Statistics.class.getName()), e);
        }
    }

    public Statistics unMarshallFromXml(String xmlStr) {
        try {
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            List<ValidationEvent> validationEvents = new ArrayList<>();
            unmarshaller.setEventHandler(event -> {
                if (event.getSeverity() == ValidationEvent.ERROR ||
                        event.getSeverity() == ValidationEvent.FATAL_ERROR) {
                    validationEvents.add(event);
                }
                //let the un-marshalling proceed to find any more problems
                return true;
            });

            Statistics statistics = (Statistics) unmarshaller.unmarshal(new StringReader(xmlStr));
            if (LOGGER.isTraceEnabled()) {
                logStatistics(statistics);
            }
            if (!validationEvents.isEmpty()) {
                String detail = validationEventsToString(validationEvents);
                throw new RuntimeException("Errors encountered un-marshalling xml: " + detail);
            }
            return statistics;
        } catch (Exception e) {
            int trimIndex = xmlStr.length() < 50 ? xmlStr.length() : 49;
            LOGGER.error("Unable to deserialise a message (enable debug to log full message): {}...", xmlStr.substring(0, trimIndex));
            LOGGER.debug("Unable to deserialise a message {}", xmlStr);
            LOGGER.error("Error un-marshalling message value");
            throw new RuntimeException(String.format("Error un-marshalling message value"), e);
        }
    }

    public String marshallToXml(Statistics statistics) {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            StringWriter stringWriter = new StringWriter();
            List<ValidationEvent> validationEvents = new ArrayList<>();
            marshaller.setEventHandler(event -> {
                if (event.getSeverity() == ValidationEvent.ERROR ||
                        event.getSeverity() == ValidationEvent.FATAL_ERROR) {
                    validationEvents.add(event);
                }
                //let the un-marshalling proceed to find any more problems
                return true;
            });
            marshaller.marshal(statistics, stringWriter);
            if (!validationEvents.isEmpty()) {
                String detail = validationEventsToString(validationEvents);
                throw new RuntimeException("Errors encountered marshalling xml: " + detail);
            }
            return stringWriter.toString();
        } catch (JAXBException e) {
            LOGGER.error("Error marshalling message value");
            throw new RuntimeException(String.format("Error marshalling message value"), e);
        }
    }

    private static String validationEventToString(final ValidationEvent validationEvent) {
        return validationEvent.getMessage() +
                " at line " +
                validationEvent.getLocator().getLineNumber() +
                " col " + validationEvent.getLocator().getColumnNumber();
    }
    private static String validationEventsToString(final List<ValidationEvent> validationEvents) {
        return validationEvents.stream()
                .map(StatisticsMarshaller::validationEventToString)
                .collect(Collectors.joining(","));
    }

    private void logStatistics(Statistics statistics) {
        statistics.getStatistic().forEach(statistic ->
                LOGGER.trace("Un-marshalling stat with time {}, count {} and value {}",
                        statistic.getTime().toString(),
                        statistic.getCount(),
                        statistic.getValue())
        );
    }
}
