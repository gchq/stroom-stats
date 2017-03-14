/*
 *
 *  * Copyright 2017 Crown Copyright
 *  *
 *  * This library is free software; you can redistribute it and/or modify it under
 *  * the terms of the GNU Lesser General Public License as published by the Free
 *  * Software Foundation; either version 2.1 of the License, or (at your option)
 *  * any later version.
 *  *
 *  * This library is distributed in the hope that it will be useful, but WITHOUT
 *  * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 *  * details.
 *  *
 *  * You should have received a copy of the GNU Lesser General Public License along
 *  * with this library; if not, write to the Free Software Foundation, Inc., 59
 *  * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *  *
 *  *
 *
 */

package stroom.stats.streams.mapping;

import org.apache.kafka.streams.KeyValue;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.hbase.uid.UniqueIdCache;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.schema.Statistics;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.StatisticWrapper;
import stroom.stats.streams.aggregation.CountAggregate;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.util.List;

public class CountStatToAggregateMapper extends AbstractStatisticMapper {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(CountStatToAggregateMapper.class);

    private final UniqueIdCache uniqueIdCache;
    private final StroomPropertyService stroomPropertyService;

    @Inject
    public CountStatToAggregateMapper(UniqueIdCache uniqueIdCache,
                                      StroomPropertyService stroomPropertyService) {
        super(uniqueIdCache, stroomPropertyService);
        this.uniqueIdCache = uniqueIdCache;
        this.stroomPropertyService = stroomPropertyService;
    }

    /**
     * Convert the Statistic object into a StatKey and a StatAggregate pair. The StatKey is a byte array representation of
     * the parts that make up the statistic key, i.e. name, tavValues. The StatAggregate is just a container for the stat value
     * ready for downstream aggregation
     */
    @Override
    public Iterable<KeyValue<StatKey, StatAggregate>> flatMap(String statName, StatisticWrapper statisticWrapper) {

        int maxEventIds = stroomPropertyService.getIntProperty(StatAggregate.PROP_KEY_MAX_AGGREGATED_EVENT_IDS, Integer.MAX_VALUE);
        Statistics.Statistic statistic = statisticWrapper.getStatistic();

        List<MultiPartIdentifier> eventIds = convertEventIds(statistic, maxEventIds);

        //convert stat value
        StatAggregate statAggregate = new CountAggregate(eventIds, statistic.getCount());
        List<KeyValue<StatKey, StatAggregate>> keyValues = buildKeyValues(statName, statisticWrapper, statAggregate);

        LOGGER.trace(() -> String.format("Flat mapping event into %s events", keyValues.size()));
        return keyValues;
    }

}
