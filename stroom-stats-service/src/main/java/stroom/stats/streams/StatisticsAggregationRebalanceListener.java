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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.util.logging.LambdaLogger;

import java.util.Collection;
import java.util.stream.Collectors;

public class StatisticsAggregationRebalanceListener implements ConsumerRebalanceListener {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(StatisticsAggregationRebalanceListener.class);

    private final StatisticsAggregationProcessor statisticsAggregationProcessor;
    private final Consumer<StatEventKey, StatAggregate> kafkaConsumer;

    public StatisticsAggregationRebalanceListener(final StatisticsAggregationProcessor statisticsAggregationProcessor,
                                                  final Consumer<StatEventKey, StatAggregate> kafkaConsumer) {

        this.statisticsAggregationProcessor = statisticsAggregationProcessor;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {

        LOGGER.debug(() -> String.format("Partitions being revoked from processor %s, current partitions [%s]",
                statisticsAggregationProcessor, extractPartitionsString(partitions)));

        //this consume is losing some of its partitions so flush everything we currently have in memory and commit the
        //offsets.  In reality we only need to flush the partitions that we are losing but as rebalance events will
        //be fairly infrequent it is simpler just to flush everything
        statisticsAggregationProcessor.flush(kafkaConsumer);
        //the assigned partitions will be recorded in the onPartitionsAssigned callback
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {

        LOGGER.debug(() -> String.format("Partitions being assigned to processor %s, new partitions [%s]",
                statisticsAggregationProcessor, extractPartitionsString(partitions)));

        //nothing to do here other than record the new partitions, the new partitions will
        //just go into the aggregator as normal
        statisticsAggregationProcessor.setAssignedPartitions(partitions);
    }

    static String extractPartitionsString(final Collection<TopicPartition> partitions) {
        return partitions.stream()
                .map(TopicPartition::partition)
                .sorted()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }
}
