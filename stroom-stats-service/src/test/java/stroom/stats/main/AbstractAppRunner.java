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

package stroom.stats.main;

import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.service.startup.App;

public abstract class AbstractAppRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAppRunner.class);

    public AbstractAppRunner() {

        try {
            App app = new App();
            app.run(new String[] {"server", "./stroom-stats-service/config_dev.yml"});

            Injector injector = app.getInjector();

            run(injector);
        } catch (Exception e) {
            LOGGER.error("Error: {}", e.getMessage(), e);
        }

        System.exit(0);
    }

    abstract void run(Injector injector) throws Exception;
}
