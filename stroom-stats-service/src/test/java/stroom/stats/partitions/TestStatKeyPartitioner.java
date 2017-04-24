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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UID;
import stroom.stats.shared.EventStoreTimeIntervalEnum;
import stroom.stats.streams.StatKey;
import stroom.stats.streams.TagValue;
import stroom.stats.streams.aggregation.StatAggregate;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

public class TestStatKeyPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestStatKeyPartitioner.class);

    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node[] nodes = new Node[]{node0, node1, node2};
    private String topic = "myTopic";
    private List<PartitionInfo> partitions = Arrays.asList(
            new PartitionInfo(topic, 1, null, nodes, nodes),
            new PartitionInfo(topic, 2, node1, nodes, nodes),
            new PartitionInfo(topic, 0, node0, nodes, nodes));

    private Cluster cluster = new Cluster(
            "myCluster",
            Arrays.asList(node0, node1, node2),
            partitions,
            Collections.<String>emptySet(),
            Collections.<String>emptySet());

    private final MockUniqueIdCache mockUniqueIdCache = new MockUniqueIdCache();

    private String statName1 = "myStatName1";
    private String statName2 = "myStatName2";
    private String tag1 = "tag1";
    private String tag2 = "tag2";
    private String tag1value1 = "tag1value1";
    private String tag1value2 = "tag1value2";
    private String tag2value1 = "tag2value1";
    private String tag2value2 = "tag2value2";
    private final ZonedDateTime baseTime = ZonedDateTime.now(ZoneOffset.UTC);
    private long time1 = baseTime.toInstant().toEpochMilli();
    private long time2 = baseTime.plus(1, ChronoUnit.MILLIS).toInstant().toEpochMilli();

    @Test
    public void testStreamPartitoner_samePartitionForSameKeyFields() throws Exception {
        StatKey statKey1a = new StatKey(
                mockUniqueIdCache.getOrCreateId(statName1),
                RollUpBitMask.ZERO_MASK,
                EventStoreTimeIntervalEnum.SECOND,
                time1,
                new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId(tag1value1)),
                new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId(tag2value1)));

        //different time but will be trunctaed to the same SECOND bucket
        StatKey statKey1b = new StatKey(
                mockUniqueIdCache.getOrCreateId(statName1),
                RollUpBitMask.ZERO_MASK,
                EventStoreTimeIntervalEnum.SECOND,
                time2,
                new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId(tag1value1)),
                new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId(tag2value1)));

        int numPartitions = 100;

        int statKey1aPartition = getStreamsPartition(statKey1a, numPartitions);
        int statKey1bPartition = getStreamsPartition(statKey1b, numPartitions);

        Assertions.assertThat(statKey1aPartition).isEqualTo(statKey1bPartition);
    }

    @Test
    public void testStreamPartitoner_singlePartition() throws Exception {
        StatKey statKey1 = new StatKey(
                mockUniqueIdCache.getOrCreateId(statName1),
                RollUpBitMask.ZERO_MASK,
                EventStoreTimeIntervalEnum.SECOND,
                time1,
                new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId(tag1value1)),
                new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId(tag2value1)));

        //most fields different but onyl one partition available
        StatKey statKey2 = new StatKey(
                mockUniqueIdCache.getOrCreateId(statName2),
                RollUpBitMask.fromShort((short) 2),
                EventStoreTimeIntervalEnum.DAY,
                time2,
                new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId(tag1value1)),
                new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId(tag2value1)));

        int numPartitions = 1;

        int statKey1Partition = getStreamsPartition(statKey1, numPartitions);
        int statKey2Partition = getStreamsPartition(statKey2, numPartitions);

        Assertions.assertThat(statKey1Partition).isEqualTo(statKey2Partition);
        Assertions.assertThat(statKey1Partition).isEqualTo(0);
    }

    @Test
    public void testProducerPartitioner_sameKeyFields() throws Exception {
        StatKey statKey1a = new StatKey(
                mockUniqueIdCache.getOrCreateId(statName1),
                RollUpBitMask.ZERO_MASK,
                EventStoreTimeIntervalEnum.SECOND,
                time1,
                new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId(tag1value1)),
                new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId(tag2value1)));

        //different time and interval
        StatKey statKey1b = new StatKey(
                mockUniqueIdCache.getOrCreateId(statName1),
                RollUpBitMask.ZERO_MASK,
                EventStoreTimeIntervalEnum.SECOND,
                time2,
                new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId(tag1value1)),
                new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId(tag2value1)));

        int numPartitions = 100;

        int statKey1aPartition = getProducerPartition(statKey1a, numPartitions);
        int statKey1bPartition = getProducerPartition(statKey1b, numPartitions);

        Assertions.assertThat(statKey1aPartition).isEqualTo(statKey1bPartition);
    }

    /**
     * test to calculate a large number of possible statKeys to make sure that the partition numbers
     * have a good spread over the partition number range.  More of a visual test for use when tweaking the
     * hashing algorithm.
     */
    @Test
    public void testStreamPartitioner_spread() {
        int numPartitions = 100;
        List<LongAdder> partitionCounters = new ArrayList<>();
        IntStream.range(0, numPartitions).forEach(i -> partitionCounters.add(new LongAdder()));

        LongAdder counter = new LongAdder();
        RollUpBitMask.getRollUpBitMasks(12).parallelStream().forEach(rollUpBitMask -> {
            Arrays.stream(EventStoreTimeIntervalEnum.values()).forEach(interval -> {
                IntStream.rangeClosed(1, 5).forEachOrdered(i -> {
                    StatKey statKey = new StatKey(
                            mockUniqueIdCache.getOrCreateId("stat-" + i),
                            rollUpBitMask,
                            interval,
                            Instant.now().toEpochMilli(),
                            new TagValue(mockUniqueIdCache.getOrCreateId(tag1), mockUniqueIdCache.getOrCreateId("tag1value" + i)),
                            new TagValue(mockUniqueIdCache.getOrCreateId(tag2), mockUniqueIdCache.getOrCreateId("tag2value" + i)));
                    counter.increment();

                    int partition = getStreamsPartition(statKey, numPartitions);

                    StatKey statKey2 = StatKey.fromBytes(statKey.getBytes());

                    int partition2 = getStreamsPartition(statKey2, numPartitions);

                    //Stat keys are identical so should have same partition
                    //Also useful test of rebuilding a statkey from bytes
                    Assertions.assertThat(partition).isEqualTo(partition2);

                    partitionCounters.get(partition).increment();
                });
            });
        });

        long totalCount = partitionCounters.stream().mapToLong(longAdder -> longAdder.sum()).sum();

        //print out the count and percentage of total for each partition value
        IntStream.range(0, numPartitions).forEachOrdered(i -> {
            long partitionCount = partitionCounters.get(i).sum();
            LOGGER.info("{} - {} - {}", i, partitionCount, ((double) (partitionCount)) / totalCount * 100);
        });
    }

    /**
     * test to calculate a large number of possible statKeys that differ only in their time component,
     * to make sure that the partition numbers have a good spread over the partition number range.
     * More of a visual test for use when tweaking the hashing algorithm.
     */
    @Test
    public void testStreamPartitioner_timeSpread() {
        int numPartitions = 10;
        List<LongAdder> partitionCounters = new ArrayList<>();
        IntStream.range(0, numPartitions).forEach(i -> partitionCounters.add(new LongAdder()));

        Instant baseTime = Instant.now();

        UID statNameUid = mockUniqueIdCache.getOrCreateId("myStat");

        LongAdder counter = new LongAdder();
        IntStream.rangeClosed(1, 50_000).forEachOrdered(i -> {
            StatKey statKey = new StatKey(
                    statNameUid,
                    RollUpBitMask.ZERO_MASK,
                    EventStoreTimeIntervalEnum.DAY,
                    baseTime.plus(i, ChronoUnit.HOURS).toEpochMilli());
            counter.increment();

            int partition = getStreamsPartition(statKey, numPartitions);

            StatKey statKey2 = StatKey.fromBytes(statKey.getBytes());

            int partition2 = getStreamsPartition(statKey2, numPartitions);

            //Stat keys are identical so should have same partition
            //Also useful test of rebuilding a statkey from bytes
            Assertions.assertThat(partition).isEqualTo(partition2);

            partitionCounters.get(partition).increment();
        });

        long totalCount = partitionCounters.stream().mapToLong(longAdder -> longAdder.sum()).sum();

        //print out the count and percentage of total for each partition value
        IntStream.range(0, numPartitions).forEachOrdered(i -> {
            long partitionCount = partitionCounters.get(i).sum();
            LOGGER.info("{} - {} - {}", i, partitionCount, ((double) (partitionCount)) / totalCount * 100);
        });

    }

    private int getProducerPartition(StatKey statKey, int numPartitions) {
        Partitioner partitioner = new StatKeyPartitioner();
        int partition = partitioner.partition(topic, statKey, statKey.getBytes(), null, null, cluster);
        Assertions.assertThat(partition).isNotNegative();
        Assertions.assertThat(partition).isLessThan(numPartitions);
        return partition;
    }

    private int getStreamsPartition(StatKey statKey, int numPartitions) {
        StreamPartitioner<StatKey, StatAggregate> partitioner = new StatKeyPartitioner();
        int partition = partitioner.partition(statKey, null, numPartitions);
        Assertions.assertThat(partition).isNotNegative();
        Assertions.assertThat(partition).isLessThan(numPartitions);
        return partition;
    }

}