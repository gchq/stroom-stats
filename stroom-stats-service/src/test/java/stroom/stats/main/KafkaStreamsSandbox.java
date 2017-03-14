/*
 *
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
 */

package stroom.stats.main;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.streams.serde.SerdeUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaStreamsSandbox {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsSandbox.class);

    public static final int MSG_COUNT = 1000;
    public static final String SOURCE_TOPIC = "sourceTopic";
    public static final String DEST_TOPIC = "destTopic";
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);

    private final ConcurrentMap<Long, AtomicLong> inputData = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, AtomicLong> outputData = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Throwable {
        new KafkaStreamsSandbox().run();
    }

    private void run() throws Throwable {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> isTerminated.set(true)));


        Properties props = new Properties();
        props.put("bootstrap.servers", "stroom.kafka:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Producer<Long, String> producer = new KafkaProducer<>(props, Serdes.Long().serializer(), Serdes.String().serializer());
//        List<Future<RecordMetadata>> futures = new ArrayList<>();
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        final AtomicLong counter = new AtomicLong(0);

        /*
        Runnable task = () -> {
            Instant now = Instant.now();
            long truncatedTime = now.truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
            inputData.computeIfAbsent(truncatedTime, aLong -> new AtomicLong()).addAndGet(1);

            ProducerRecord<Long, String> rec = new ProducerRecord<>(
                    SOURCE_TOPIC,
                    now.toEpochMilli(),
                    "1");
            try {
                producer.send(rec);
            } catch (Exception e) {
                LOGGER.error("Error trying to send rec {} to topic {}", rec, SOURCE_TOPIC, e);
                throw e;
            }
            LOGGER.info("Sending rec key: {} val: {}", epochMsToString(rec.key()), rec.value());
//            }
        };
        */


        //Each interval send a msg
//        ScheduledFuture scheduledFuture = executorService.scheduleAtFixedRate(task, 0, 100, TimeUnit.MILLISECONDS);

        LOGGER.info("Scheduled producer started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            producer.close();
        }));

        //wait for the submission of the single task
//        exFuture.get();

//        LOGGER.info("Futures count {}", futures.size());

//        //wait for each message to be sent
//        futures.forEach(future -> {
//            try {
//                future.get();
//            } catch (InterruptedException e) {
//                throw new RuntimeException("Interrupted!!!", e);
//            } catch (ExecutionException e) {
//                throw new RuntimeException("Something went wrong", e);
//            }
//        });


        //start up a consumer on a different thread to log the content of DEST_TOPIC
        consume();

        //start the stream processor on this thread, runs forever
        stream();
        Random random = new Random();

        IntStream.rangeClosed(1, 30_000)
                .forEach((i) -> {
                    Instant now = Instant.now();
                    long truncatedTime = now.truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
                    inputData.computeIfAbsent(truncatedTime, aLong -> new AtomicLong()).addAndGet(1);

                    ProducerRecord<Long, String> rec = new ProducerRecord<>(
                            SOURCE_TOPIC,
                            now.toEpochMilli(),
                            "1");
                    try {
                        producer.send(rec);
                    } catch (Exception e) {
                        LOGGER.error("Error trying to send rec {} to topic {}", rec, SOURCE_TOPIC, e);
                        throw e;
                    }
//                    LOGGER.info("Sending rec key: {} val: {}", epochMsToString(rec.key()), rec.value());
                    try {
                        Thread.sleep(random.nextInt(3));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(String.format("Interrupted"), e);
                    }

                });

        LOGGER.info("Sent all messages");

        Thread.sleep(40_000);

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            inputData.keySet().stream()
                    .sorted()
                    .forEach(key -> {
                        String inputCnt = getVal(key, inputData);
                        String outputCnt = getVal(key, outputData);
                        LOGGER.info(String.format("Key: %s input: %s output: %s %s", epochMsToString(key), inputCnt, outputCnt, inputCnt.equals(outputCnt) ? "" : "DIFF"));
                    });

            long totalInput = inputData.values().stream().mapToLong(AtomicLong::get).sum();
            long totalOutput = outputData.values().stream().mapToLong(AtomicLong::get).sum();
            LOGGER.info("Input total: {} Output total: {}", totalInput, totalOutput);

        }, 0, 10, TimeUnit.SECONDS);
        executorService.shutdown();


    }

    private String getVal(long key, ConcurrentMap<Long, AtomicLong> map) {
        AtomicLong atomicLong = map.get(key);
        if (atomicLong == null) {
            return "";
        } else {
            return Long.toString(atomicLong.get());
        }
    }

    private KafkaStreams stream() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "streamProcessorClientId");
        props.put("group.id", "streamProcessorGroup");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamProcessorAppId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "stroom.kafka:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        //We want to batch msgs by stream time, not
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        //The commit interval controls the rate at which kafka commits what it has done so far,
        //i.e. putting processed output on an output queue
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 4000);
//        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class.getName());

        //10MB cache - see http://docs.confluent.io/3.1.2/streams/developer-guide.html#memory-management
        //This cache controls how much of the aggregation can be done in memory before having to persist
        //state to rocksDb
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024L);

        //Number of threads used by the stream processor
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);


        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
//        LongAggregatorSerializer longAggregatorSerializer = new LongAggregatorSerializer();
//        LongAggregatorDeserializer longAggregatorDeserializer = new LongAggregatorDeserializer();
//        Serde<LongAggregator> longAggregatorSerde = Serdes.serdeFrom(longAggregatorSerializer, longAggregatorDeserializer);
        Serde<LongAggregator> longAggregatorSerde = SerdeUtils.buildBasicSerde(
                (topic, data) -> Bytes.toBytes(data.getAggregateVal()),
                (topic, bData) -> new LongAggregator(Bytes.toLong(bData)));

        SerdeUtils.verify(longAggregatorSerde, new LongAggregator(123));


//        StringSerializer stringSerializer = new StringSerializer();
//        StringDeserializer stringDeserializer = new StringDeserializer();
        WindowedSerializer<Long> longWindowedSerializer = new WindowedSerializer<>(longSerde.serializer());
        WindowedDeserializer<Long> longWindowedDeserializer = new WindowedDeserializer<>(longSerde.deserializer());
        Serde<Windowed<Long>> windowedSerde = Serdes.serdeFrom(longWindowedSerializer, longWindowedDeserializer);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, String> stream = builder.stream(longSerde, stringSerde, SOURCE_TOPIC);

        TimeWindows timeWindows = TimeWindows.of(1000);
        Windows<TimeWindow> windows = timeWindows.until(0);

        stream
                .selectKey((key, value) -> Instant.ofEpochMilli(key).truncatedTo(ChronoUnit.SECONDS).toEpochMilli())
                .groupByKey(longSerde, stringSerde)
                .aggregate(
                        LongAggregator::new,
                        (aggKey, value, longAggregator) -> longAggregator.add(Long.valueOf(value)),
                        windows,
                        longAggregatorSerde,
                        "aggregates")
//                .toStream((wk, v) -> wk.key())
                .toStream()
                .process(() -> {
                    return new Processor<Windowed<Long>, LongAggregator>() {
                        Windowed<Long> lastKey = null;
                        long lastVal = 0;
                        @Override
                        public void init(ProcessorContext context) {


                        }

                        @Override
                        public void process(Windowed<Long> key, LongAggregator value) {
                            long val;
                            //Each msg is an an aggregate (grouped by event key) over time windows defined
                            //in stream processor wall clock time (not event time). If a commit happens then
                            //the aggregate for a bucket may be put on the stream partially full. If this happens
                            //then the next msg will be the remainder. We are always dealing with deltas rather
                            //than absolute values so we need to convert an updated value of a window bucket
                            //into a delta from the last one we have seen.
                            if (lastKey == null || !lastKey.equals(key)) {
                                val = value.getAggregateVal();
                            } else {
                                val = value.getAggregateVal() - lastVal;
                            }
                            lastKey = key;
                            lastVal = value.getAggregateVal();

                            LOGGER.info("Processing message key: {} winStart: {} winEnd {} winDuration: {} val: {}",
                                    epochMsToString(key.key()),
                                    epochMsToString(key.window().start()),
                                    epochMsToString(key.window().end()),
                                    key.window().end() - key.window().start(),
                                    val);
//                                    value.getAggregateVal());

                            outputData.computeIfAbsent(key.key(), aLong -> new AtomicLong()).addAndGet(val);
//                            outputData.computeIfAbsent(key.key(), aLong -> new AtomicLong()).addAndGet(value.getAggregateVal());
                        }

                        @Override
                        public void punctuate(long timestamp) {

                        }

                        @Override
                        public void close() {

                        }
                    };
                });
//                .to(windowedSerde, longAggregatorSerde, DEST_TOPIC);
//                .to(longSerde, longAggregatorSerde, DEST_TOPIC);


        LOGGER.info("Starting stream processor");
        KafkaStreams kafkaStreams = new KafkaStreams(builder, new StreamsConfig(props));
        kafkaStreams.setUncaughtExceptionHandler((t, e) -> LOGGER.error("t: {} e: {}", t, e));
        kafkaStreams.cleanUp();
        kafkaStreams.start();
//        kafkaStreams.store("aggregates", QueryableStoreTypes.windowStore());
        LOGGER.info("Stream processor started");

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

//        while (true) {
//        while (!isTerminated.get()) {
        //infinite loop to prevent application from shutting down
//        }

//        kafkaStreams.close();
//        LOGGER.info("Stream processor closed");
        return kafkaStreams;
    }

    private void consume() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "stroom.kafka:9092");
        consumerProps.put("group.id", "consumerGroup");
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
//        LongAggregatorSerializer longAggregatorSerialiser = new LongAggregatorSerializer();
//        LongAggregatorDeserializer longAggregatorDeserialiser = new LongAggregatorDeserializer();
//        Serde<LongAggregator> longAggregatorSerde = Serdes.serdeFrom(longAggregatorSerialiser, longAggregatorDeserialiser);
        Serde<LongAggregator> longAggregatorSerde = SerdeUtils.buildBasicSerde(
                (topic, data) -> Bytes.toBytes(data.getAggregateVal()),
                (topic, bData) -> new LongAggregator(Bytes.toLong(bData)));

        SerdeUtils.verify(longAggregatorSerde, new LongAggregator(123));

        WindowedSerializer<Long> longWindowedSerializer = new WindowedSerializer<>(longSerde.serializer());
        WindowedDeserializer<Long> longWindowedDeserializer = new WindowedDeserializer<>(longSerde.deserializer());
        Serde<Windowed<Long>> windowedSerde = Serdes.serdeFrom(longWindowedSerializer, longWindowedDeserializer);

        KafkaConsumer<Windowed<Long>, LongAggregator> consumer = new KafkaConsumer<>(
                consumerProps,
                windowedSerde.deserializer(),
//                longSerde.deserializer(),
                longAggregatorSerde.deserializer());

        consumer.subscribe(Collections.singletonList(DEST_TOPIC));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future future = executorService.submit(() -> {
            LOGGER.info("Consumer about to poll");
            Instant terminationTime = null;
//            while (!isTerminated.get() || Instant.now().isBefore(terminationTime.plusSeconds(10))) {
            while (true) {
                try {
//                    ConsumerRecords<Windowed<Long>, LongAggregator> records = consumer.poll(100);
                    ConsumerRecords<Windowed<Long>, LongAggregator> records = consumer.poll(100);
//                LOGGER.info("Received {} messages in batch", records.count());
                    for (ConsumerRecord<Windowed<Long>, LongAggregator> record : records) {
//                    for (ConsumerRecord<Long, LongAggregator> record : records) {
                        //                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                        LOGGER.info("Received message key: {} winStart: {} winEnd {} winDuration: {} val: {}",
                                epochMsToString(record.key().key()),
                                epochMsToString(record.key().window().start()),
                                epochMsToString(record.key().window().end()),
                                record.key().window().end() - record.key().window().start(),
                                record.value().getAggregateVal());
//                        LOGGER.info("Received message key: {} val: {}",
//                                epochMsToString(record.key()),
//                                record.value().getAggregateVal());
//                        outputData.computeIfAbsent(record.key(),aLong -> new AtomicLong()).addAndGet(record.value().getAggregateVal());
                        outputData.computeIfAbsent(record.key().key(), aLong -> new AtomicLong()).addAndGet(record.value().getAggregateVal());
                    }
                } catch (Exception e) {
                    LOGGER.error("Error polling topic {} ", DEST_TOPIC, e);
                }
                if (isTerminated.get()) {
                    terminationTime = Instant.now();
                }
            }
//            consumer.close();
//            LOGGER.info("Consumer closed");

        });
        LOGGER.info("Consumer started");
    }

    private String epochMsToString(long epochMs) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC).toString();
    }

    public static class LongAggregator {

        private static final Logger LOGGER = LoggerFactory.getLogger(LongAggregator.class);

        private long aggreagteVal;

        public LongAggregator() {
            this.aggreagteVal = 0;
        }

        public LongAggregator(final long aggreagteVal) {
            this.aggreagteVal = aggreagteVal;
        }

        public LongAggregator add(final long val) {
//            return new LongAggregator(val + aggreagteVal);
            LOGGER.trace("Adding {} to {}", val, aggreagteVal);
            aggreagteVal += val;
            return this;
        }

        public long getAggregateVal() {
            return aggreagteVal;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final LongAggregator that = (LongAggregator) o;

            return aggreagteVal == that.aggreagteVal;
        }

        @Override
        public int hashCode() {
            return (int) (aggreagteVal ^ (aggreagteVal >>> 32));
        }

        @Override
        public String toString() {
            return "LongAggregator{" +
                    "aggreagteVal=" + aggreagteVal +
                    '}';
        }
    }

    public static class LongAggregatorSerializer implements Serializer<LongAggregator> {

        private static final Logger LOGGER = LoggerFactory.getLogger(LongAggregatorSerializer.class);

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public byte[] serialize(final String topic, final LongAggregator data) {
            LOGGER.trace("serialising {} on topic {}", data.getAggregateVal(), topic);
            return Bytes.toBytes(data.getAggregateVal());
        }

        @Override
        public void close() {

        }
    }

    public static class LongAggregatorDeserializer implements Deserializer<LongAggregator> {

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {

        }

        @Override
        public LongAggregator deserialize(final String topic, final byte[] data) {
            LOGGER.trace("deserialising {} on topic {}", Bytes.toLong(data), topic);
            return new LongAggregator(Bytes.toLong(data));
        }

        @Override
        public void close() {

        }
    }


    public static class EventTimeExtractor implements TimestampExtractor {

        //kafka 0.10.2.0
//        @Override
//        public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
//            return (long) record.key();
//        }

        //kafka 0.10.1.0
        @Override
        public long extract(final ConsumerRecord<Object, Object> record) {
            return (long) record.key();
        }
    }


}
