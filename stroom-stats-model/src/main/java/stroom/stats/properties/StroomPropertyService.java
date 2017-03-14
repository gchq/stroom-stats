/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.properties;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;

public interface StroomPropertyService {

    Optional<String> getProperty(final String name);

    /**
     * Sets the property with name name to value value, creates the property
     * if it doesn't already exist
     * @param name property name
     * @param value property value
     */
    void setProperty(final String name, final String value);

    List<String> getAllPropertyKeys();

    /**
     * Dumps all properties using the supplied logMethod
     */
    default void dumpAllProperties(final Consumer<String> logMethod) {
        getAllPropertyKeys().forEach(key -> {
            String msg = String.format("%s - %s", key, getProperty(key,""));
            logMethod.accept(msg);
        });
    }

    //TODO the conversion from OptString to OptInt is a bit grim but will
    //suffice till the implementation of the property service is ironed out

    default String getPropertyOrThrow(String name) {
        return getProperty(name).orElseThrow(() -> new PropertyDoesNotExistException(name));
    }

    default String getProperty(String name, String defaultValue) {
       return getProperty(name).orElse(defaultValue);
    }

    default OptionalInt getIntProperty(String name) {
        Optional<String> optValue = getProperty(name);
        if (optValue.isPresent()) {
            return OptionalInt.of(Integer.parseInt(optValue.get()));
        } else {
            return OptionalInt.empty();
        }
    }

    default int getIntProperty(String name, int defaultValue) {
        return getIntProperty(name).orElse(defaultValue);
    }

    default int getIntPropertyOrThrow(String name) {
        return getIntProperty(name).orElseThrow(() -> new PropertyDoesNotExistException(name));
    }

    default OptionalLong getLongProperty(String name) {
        Optional<String> optValue = getProperty(name);
        if (optValue.isPresent()) {
            return OptionalLong.of(Long.parseLong(optValue.get()));
        } else {
            return OptionalLong.empty();
        }
    }

    default long getLongProperty(String name, long defaultValue) {
        return getLongProperty(name).orElse(defaultValue);
    }

    default long getLongPropertyOrThrow(String name) {
        return getLongProperty(name).orElseThrow(() -> new PropertyDoesNotExistException(name));
    }

    default OptionalDouble getDoubleProperty(String name) {
        Optional<String> optValue = getProperty(name);
        if (optValue.isPresent()) {
            return OptionalDouble.of(Double.parseDouble(optValue.get()));
        } else {
            return OptionalDouble.empty();
        }
    }

    default double getDoubleProperty(String name, double defaultValue) {
        return getDoubleProperty(name).orElse(defaultValue);
    }

    default double getDoublePropertyOrThrow(String name) {
        return getDoubleProperty(name).orElseThrow(() -> new PropertyDoesNotExistException(name));
    }

    default Optional<Boolean> getBooleanProperty(String name) {
        Optional<String> optValue = getProperty(name);
        return Optional.ofNullable(Boolean.valueOf(optValue.orElse(null)));
    }

    default boolean getBooleanProperty(String name, boolean defaultValue) {
        Optional<String> optValue = getProperty(name);
        if (optValue.isPresent()) {
            return Boolean.getBoolean(optValue.get());
        } else {
            return defaultValue;
        }
    }

    default boolean getBooleanPropertyOrThrow(String name) {
        return getBooleanProperty(name).orElseThrow(() -> new PropertyDoesNotExistException(name));
    }

    default void setProperty(final String name, final int value) {
        setProperty(name, Integer.toString(value));
    }

    default void setProperty(final String name, final long value) {
        setProperty(name, Long.toString(value));
    }

    default void setProperty(final String name, final double value) {
        setProperty(name, Double.toString(value));
    }
}
