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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessingStartStopIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingStartStopIT.class);

    private Injector injector = getApp().getInjector();

    @Test
    public void testRunAppStopStartProcessing() {


        req().stopProcessing();

        req().startProcessing();


//        //TODO do a POST to the stopProcessing task (eg. curl -X POST http://localhost:8086/admin/tasks/stopProcessing)
//        //then check it is down,
//        //then do a POST to startProcessing and check it is running
//
//        while (true) {
//
//        }

    }

}
