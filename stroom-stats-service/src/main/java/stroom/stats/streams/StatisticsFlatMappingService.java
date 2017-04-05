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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.mixins.Startable;
import stroom.stats.mixins.Stoppable;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.streams.mapping.CountStatToAggregateFlatMapper;
import stroom.stats.streams.mapping.ValueStatToAggregateFlatMapper;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class StatisticsFlatMappingService implements Startable, Stoppable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsFlatMappingService.class);

    private final StroomPropertyService stroomPropertyService;
    private final StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory;
    private final CountStatToAggregateFlatMapper countStatToAggregateMapper;
    private final ValueStatToAggregateFlatMapper valueStatToAggregateMapper;

    private final List<StatisticsFlatMappingProcessor> processors = new ArrayList<>();

    @Inject
    public StatisticsFlatMappingService(final StroomPropertyService stroomPropertyService,
                                        final StatisticsFlatMappingStreamFactory statisticsFlatMappingStreamFactory,
                                        final CountStatToAggregateFlatMapper countStatToAggregateMapper,
                                        final ValueStatToAggregateFlatMapper valueStatToAggregateMapper) {

        this.stroomPropertyService = stroomPropertyService;
        this.statisticsFlatMappingStreamFactory = statisticsFlatMappingStreamFactory;
        this.countStatToAggregateMapper = countStatToAggregateMapper;
        this.valueStatToAggregateMapper = valueStatToAggregateMapper;

        StatisticsFlatMappingProcessor countStatisticsProcessor = new StatisticsFlatMappingProcessor(
                stroomPropertyService,
                statisticsFlatMappingStreamFactory,
                StatisticType.COUNT,
                countStatToAggregateMapper);
        processors.add(countStatisticsProcessor);

        StatisticsFlatMappingProcessor valueStatisticsProcessor = new StatisticsFlatMappingProcessor(
                stroomPropertyService,
                statisticsFlatMappingStreamFactory,
                StatisticType.VALUE,
                valueStatToAggregateMapper);
        processors.add(valueStatisticsProcessor);

    }

    @Override
    public void start() {

//        startFlatMapProcessing();

        processors.forEach(processor ->
                processor.start());
    }

    @Override
    public void stop() {
        processors.forEach(processor ->
                processor.stop());
    }

//    private void startFlatMapProcessing() {
//        LOGGER.info("Starting count flat map processor");
//        KafkaStreams countFlatMapProcessor = startFlatMapProcessor(
//                StatisticType.COUNT,
//                countStatToAggregateMapper);
//
//        LOGGER.info("Starting value flat map processor");
//        KafkaStreams valueFlatMapProcessor = startFlatMapProcessor(
//                StatisticType.VALUE,
//                valueStatToAggregateMapper);
//    }





}
