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

package stroom.stats.streams;

import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.util.healthchecks.HasHealthCheck;
import stroom.stats.util.HasRunState;
import stroom.stats.util.Startable;
import stroom.stats.util.Stoppable;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.streams.mapping.CountStatToAggregateFlatMapper;
import stroom.stats.streams.mapping.ValueStatToAggregateFlatMapper;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class StatisticsFlatMappingService implements Startable, Stoppable, HasRunState, HasHealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsFlatMappingService.class);

    private final List<StatisticsFlatMappingProcessor> processors = new ArrayList<>();

    private volatile RunState runState = RunState.STOPPED;

    //used for thread synchronization
    private final Object startStopMonitor = new Object();

    @Inject
    public StatisticsFlatMappingService(final StroomPropertyService stroomPropertyService,
                                        final StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory,
                                        final CountStatToAggregateFlatMapper countStatToAggregateMapper,
                                        final ValueStatToAggregateFlatMapper valueStatToAggregateMapper) {

        StatisticsFlatMappingProcessor countStatisticsProcessor = new StatisticsFlatMappingProcessor(
                stroomPropertyService,
                topicDefinitionFactory, statisticsFlatMappingStreamFactory,
                StatisticType.COUNT,
                countStatToAggregateMapper);
        processors.add(countStatisticsProcessor);

        StatisticsFlatMappingProcessor valueStatisticsProcessor = new StatisticsFlatMappingProcessor(
                stroomPropertyService,
                topicDefinitionFactory, statisticsFlatMappingStreamFactory,
                StatisticType.VALUE,
                valueStatToAggregateMapper);
        processors.add(valueStatisticsProcessor);
    }

    @Override
    public void start() {

        synchronized (startStopMonitor) {
            runState = RunState.STARTING;
            LOGGER.info("Starting the Statistics Flat Mapping Service");

            processors.forEach(StatisticsFlatMappingProcessor::start);
            runState = RunState.RUNNING;
        }
    }

    @Override
    public void stop() {
        synchronized (startStopMonitor) {
            runState = RunState.STOPPING;
            LOGGER.info("Stopping the Statistics Flat Mapping Service");

            processors.forEach(StatisticsFlatMappingProcessor::stop);
            runState = RunState.STOPPED;
        }
    }

    @Override
    public RunState getRunState() {
        return runState;
    }

    @Override
    public HealthCheck.Result getHealth() {
        HealthCheck.ResultBuilder builder = HealthCheck.Result.builder();
        long nonRunningProcessorCount = processors.stream()
                .filter(processor -> !processor.getRunState().equals(RunState.RUNNING))
                .count();
        if (!runState.equals(RunState.RUNNING) || nonRunningProcessorCount > 0) {
            builder.unhealthy();
        } else {
            builder.healthy();
        }
        builder.withDetail("runState", runState.name());
        builder.withDetail("processorCount", processors.size());
        builder.withDetail("processors", processors.stream()
                .collect(HasHealthCheck.buildTreeMapCollector(
                        StatisticsFlatMappingProcessor::getName,
                        StatisticsFlatMappingProcessor::produceHealthCheckSummary)));

        return builder.build();
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    private String produceHealthCheckSummary() {
        return new StringBuilder()
                .append(runState)
                .append(" - ")
                .append("Processors: ")
                .append(processors.size())
                .toString();
    }

    public List<HasHealthCheck> getHealthCheckProviders() {
        List<HasHealthCheck> healthCheckProviders = new ArrayList<>();
        processors.forEach(healthCheckProviders::add);
        return healthCheckProviders;
    }

}
