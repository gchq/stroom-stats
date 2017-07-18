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

import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.HeadlessTestApp;
import stroom.stats.service.config.Config;

import java.time.Duration;

public abstract class AbstractHeadlessIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHeadlessIT.class);

    @ClassRule
    public static DropwizardAppRule<Config> appRule = new DropwizardAppRule<>(
            HeadlessTestApp.class, "config.yml");

    private HeadlessTestApp app;

    @Before
    public void setupTest() {
        app = appRule.getApplication();
    }

    public HeadlessTestApp getApp() {
        return app;
    }


    public void sleep(final Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted during sleep");
            throw new RuntimeException("Thread interrupted during sleep");
        }
    }
}