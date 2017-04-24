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

package stroom.stats.properties;

import stroom.stats.util.logging.LambdaLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockStroomPropertyService implements StroomPropertyService {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(MockStroomPropertyService.class);

    Map<String, String> properties = new ConcurrentHashMap<>();

    private static final String PROPERTY_DEFAULTS_FILE_NAME = "mock-stroom-stats.properties";

    public MockStroomPropertyService() {
        loadProperties();
    }

    private void loadProperties() {

        InputStream inputStream = MockStroomPropertyService.class.getClassLoader().getResourceAsStream(PROPERTY_DEFAULTS_FILE_NAME);

        if (inputStream == null) {
            throw new RuntimeException(String.format("Could not find resource %s", PROPERTY_DEFAULTS_FILE_NAME));
        }

        Properties defaultProps = new Properties();
        try {
            defaultProps.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error opening file %s", PROPERTY_DEFAULTS_FILE_NAME), e);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error initialising property keys from file %s", PROPERTY_DEFAULTS_FILE_NAME), e);
            }
        }

        //Ensure we have all the required property keys, with default values if not already set
        defaultProps.stringPropertyNames().stream()
                .sorted()
                .forEach(name -> setProperty(name, defaultProps.getProperty(name)));
    }

    @Override
    public void setProperty(String name, String value) {
        LOGGER.debug("Setting property {} to {}", name, value);
        properties.put(name, value);
    }

    @Override
    public Optional<String> getProperty(final String name) {
        return Optional.ofNullable(properties.get(name));
    }

    @Override
    public List<String> getAllPropertyKeys() {
        return properties.keySet().stream().collect(Collectors.toList());
    }

    /**
     * Remove all properties
     */
    public void clearAll() {
        properties.clear();
    }

    /**
     * Remove a single named property
     */
    public void remove(String name) {
        properties.remove(name);
    }

    /**
     * Clear the existing properties and reload the values from the file
     */
    public void reloadAll() {
        clearAll();
        loadProperties();
    }

}
