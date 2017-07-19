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
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.util.healthchecks.HasHealthCheck;
import stroom.stats.util.HasRunState;
import stroom.stats.streams.StatisticsFlatMappingService;

import java.time.Instant;

public class ProcessingStartStopIT extends AbstractAppIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingStartStopIT.class);

    private Injector injector = getApp().getInjector();
    StatisticsFlatMappingService statisticsFlatMappingService = injector.getInstance(StatisticsFlatMappingService.class);
    StatisticsAggregationService statisticsAggregationService = injector.getInstance(StatisticsAggregationService.class);

    @Test
    public void testRunAppStopStartProcessing() throws InterruptedException {


        assertStates(HasRunState.RunState.RUNNING, true);

        req().stopProcessing();

        assertStates(HasRunState.RunState.STOPPED, false);

        req().startProcessing();

        assertStates(HasRunState.RunState.RUNNING, true);

//        while (true) {
//            ThreadUtil.sleep(100);
//        }
    }

    private void assertStates(HasRunState.RunState expectedRunState, boolean expectedIsHealthy) throws InterruptedException {

        assertState(statisticsFlatMappingService, expectedRunState, 10_000);
        statisticsFlatMappingService.getHealthCheckProviders().forEach(hasHealthCheck ->
                assertState(hasHealthCheck, expectedIsHealthy, 10_000));

        assertState(statisticsAggregationService, expectedRunState, 10_000);
        statisticsAggregationService.getHealthCheckProviders().forEach(hasHealthCheck ->
                assertState(hasHealthCheck, expectedIsHealthy, 10_000));

    }


    private void assertState(HasRunState hasRunState, HasRunState.RunState expectedRunState, int timeoutMs) {
        Instant timeoutTime = Instant.now().plusMillis(timeoutMs);
        while (!hasRunState.getRunState().equals(expectedRunState) && Instant.now().isBefore(timeoutTime)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Assertions.assertThat(hasRunState.getRunState()).isEqualTo(expectedRunState);
    }

    private void assertState(HasHealthCheck hasHealthCheck, boolean expectedIsHealthy, int timeoutMs) {
        Instant timeoutTime = Instant.now().plusMillis(timeoutMs);
        while (hasHealthCheck.getHealth().isHealthy() != expectedIsHealthy && Instant.now().isBefore(timeoutTime)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Assertions.assertThat(hasHealthCheck.getHealth().isHealthy()).isEqualTo(expectedIsHealthy);
    }

}
