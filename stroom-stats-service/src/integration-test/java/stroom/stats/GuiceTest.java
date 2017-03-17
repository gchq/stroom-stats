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

package stroom.stats;

import com.google.inject.Injector;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.properties.AbstractHeadlessIT;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class GuiceTest extends AbstractHeadlessIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(GuiceTest.class);

    @Test
    public void testGuice() {
        Injector injector = getApp().getInjector();

        //test all the constructors to make sure guice can bind them
        findConstructors(injector::getProvider, "stroom.stats", javax.inject.Inject.class);
        findConstructors(injector::getProvider, "stroom.stats", com.google.inject.Inject.class);
    }

    private void findConstructors(Consumer<Class<?>> actionPerClass, String packagePrefix, Class<? extends Annotation> annotationClass) {
        LOGGER.info("Finding all classes in {} with {} constructors",
                packagePrefix, annotationClass.getCanonicalName());

        List<Class> injectableClass = new ArrayList<>();
        new FastClasspathScanner(packagePrefix)
                .matchClassesWithMethodAnnotation(
                        annotationClass,
                        (matchingClass, matchingMethod) -> {
                            injectableClass.add(matchingClass);
                        })
                .scan();

        injectableClass.forEach(clazz -> {
            actionPerClass.accept(clazz);
            LOGGER.info(clazz.getCanonicalName());
        });
    }



}
