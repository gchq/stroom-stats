

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

package stroom.stats.hbase;

import stroom.query.api.Query;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticType;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.common.StatisticDataSet;
import stroom.stats.common.StatisticConfigurationValidator;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.server.common.AbstractStatisticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.aggregation.AggregatedEvent;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is the entry point for all interactions with the HBase backed statistics store, e.g.
 * putting events, searching for data, purging data etc.
 */
public class HBaseStatisticsService extends AbstractStatisticsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStatisticsService.class);

    public static final String ENGINE_NAME = "hbase";

    private final EventStores eventStores;

    @Inject
    public HBaseStatisticsService(final StatisticConfigurationService statisticConfigurationService,
                                  final StatisticConfigurationValidator statisticConfigurationValidator,
                                  final EventStores eventStores,
                                  final StroomPropertyService propertyService) {
        super(statisticConfigurationValidator, statisticConfigurationService, propertyService);

        LOGGER.debug("Initialising: {}", this.getClass().getCanonicalName());

        this.eventStores = eventStores;
    }


    @Override
    public void putAggregatedEvents(final StatisticType statisticType,
                                    final EventStoreTimeIntervalEnum interval,
                                    final List<AggregatedEvent> aggregatedEvents) {
        //TODO
        //Think we need to change the javadoc on this method to state that message should belong to the same stat type
        //and interval. This is is to save having to group them by type/interval again if they we already divided up
        //that way in the topics.
        //Change EventStores to take a list of AggregatedEvents
        //Change EventStore to take a list of AggregatedEvents
        //in HBaseEventStoreTable change to take a list of AggregatedEvents and convert these into CellQualifiers
        //revisit buffering in HBEST as we want to consume a batch, put that batch then commit kafka
        //kafka partitioning should mean similarity between stat keys in the batch
        eventStores.putAggregatedEvents(statisticType, interval, aggregatedEvents);
    }

    @Override
    public boolean putEvents(final StatisticConfiguration statisticConfiguration, final List<StatisticEvent> statisticEvents) {

//        if (validateStatisticConfiguration(statisticEvents.iterator().next(), statisticConfiguration) == false) {
//            // no StatisticConfiguration entity so don't record the stat as we
//            // will have no way of querying the stat
//            return false;
//        }

        final List<RolledUpStatisticEvent> rolledUpStatisticEvents = statisticEvents.stream()
                .map(statisticEvent -> generateTagRollUps(statisticEvent, statisticConfiguration))
                .collect(Collectors.toList());

        eventStores.putEvents(rolledUpStatisticEvents, statisticConfiguration.getPrecision(), statisticConfiguration.getStatisticType());

        return true;
    }

    @Override
    public StatisticDataSet searchStatisticsData(final Query query, final StatisticConfiguration dataSource) {
        final FindEventCriteria criteria = buildCriteria(query, dataSource);
        return eventStores.getStatisticsData(criteria, dataSource);
    }

    // @Override
    // public void refreshMetadata() {
    // statStoreMetadataService.refreshMetadata();
    // }

    @Override
    public List<String> getValuesByTag(final String tagName) {
        // TODO This will be used for providing a dropdown of known values in
        // the UI
        throw new UnsupportedOperationException("Code waiting to be written");
    }

    @Override
    public List<String> getValuesByTagAndPartialValue(final String tagName, final String partialValue) {
        // TODO This will be used for auto-completion in the UI
        throw new UnsupportedOperationException("Code waiting to be written");
    }

    @Override
    public void purgeOldData(final List<StatisticConfiguration> statisticConfigurations) {
        eventStores.purgeOldData(statisticConfigurations);

    }

    @Override
    public void purgeAllData(final List<StatisticConfiguration> statisticConfigurations) {
        eventStores.purgeStatisticStore(statisticConfigurations);

    }

    @Override
    public void flushAllEvents() {
        eventStores.flushAllEvents();
    }

    @Deprecated
    public EventStores getEventStoresForTesting() {
        return eventStores;
    }

    @Override
    public void shutdown() {
        flushAllEvents();
    }
}
