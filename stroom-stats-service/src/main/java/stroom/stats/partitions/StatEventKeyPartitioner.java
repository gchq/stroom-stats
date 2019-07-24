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

package stroom.stats.partitions;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.streams.StatEventKey;
import stroom.stats.streams.aggregation.StatAggregate;

import java.util.List;
import java.util.Map;

/**
 * Custom partitioner of a {@link StatEventKey} instance for use with both a Kafka Producer and a Kafka Streams application
 */
public class StatEventKeyPartitioner implements Partitioner, StreamPartitioner<StatEventKey, StatAggregate> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatEventKeyPartitioner.class);

    private static final StatEventKeyPartitioner INSTANCE = new StatEventKeyPartitioner();

    public static StatEventKeyPartitioner instance() {
        //Stateless so hold a common instance for all to use
        return INSTANCE;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> configs) {
    }

    @Override
    public int partition(final String topic, final Object key, final byte[] keyBytes, final Object value, final byte[] valueBytes, final Cluster cluster) {
        StatEventKey statEventKey = (StatEventKey) Preconditions.checkNotNull(key);
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        int partition = partition(statEventKey, numPartitions);
        LOGGER.trace("partition called for topic {}, key {}, numPartitions {}, returning {}", topic, key, numPartitions, partition);

        return partition;
    }

    @Override
    public Integer partition(final String topic,
                             final StatEventKey key,
                             final StatAggregate value,
                             final int numPartitions) {
        int partition = partition(key, numPartitions);
        LOGGER.trace("partition called for key {}, numPartitions {}, returning {}", key, numPartitions, partition);

        return partition;
    }

    private static int partition(StatEventKey statEventKey, final int numPartitions) {
        //the time portion of stat key has already been truncated to its interval so
        //keys that are valid for aggregation together will have the same hascode

        //As the hashcode has been cached on the StatEventKey we can just use that rather than recomputing one.
        //Need to ensure it is non-negative though
        int positiveHashCode = statEventKey.hashCode() & 0x7fffffff;

        return positiveHashCode % numPartitions;
    }

//    private static int partition(StatEventKey statKey, final int numPartitions) {
//        UID statNameUid = statKey.getStatUuid();
//        Hasher hasher = Hashing.murmur3_32().newHasher()
//                .putBytes(statNameUid.getBackingArray(), statNameUid.getOffset(), UID.UID_ARRAY_LENGTH)
//                .putBytes(statKey.getRollupMask().asBytes());
//
//        statKey.getTagValues().forEach(tagValue -> {
//            UID tag = tagValue.getTag();
//            UID value = tagValue.getValue();
//            hasher.putBytes(tag.getBackingArray(), tag.getOffset(), UID.UID_ARRAY_LENGTH);
//            hasher.putBytes(value.getBackingArray(), value.getOffset(), UID.UID_ARRAY_LENGTH);
//        });
//        //ensure hash is non-negative
//        int hash = hasher.hash().asInt() >>> 1;
//        return hash % numPartitions;
//    }
}
