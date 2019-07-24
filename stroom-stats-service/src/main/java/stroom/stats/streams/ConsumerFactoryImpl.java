package stroom.stats.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import stroom.stats.properties.StroomPropertyService;
import stroom.stats.util.logging.LambdaLogger;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static stroom.stats.streams.StatisticsAggregationProcessor.PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS;

public class ConsumerFactoryImpl implements ConsumerFactory {

   private static final LambdaLogger LOGGER = LambdaLogger.getLogger(ConsumerFactoryImpl.class);

   public static final String PROP_KEY_AGGREGATOR_POLL_RECORDS = "stroom.stats.aggregation.pollRecords";

   private final StroomPropertyService stroomPropertyService;

   @Inject
   public ConsumerFactoryImpl(final StroomPropertyService stroomPropertyService) {
      this.stroomPropertyService = stroomPropertyService;
   }

   public <K, V> Consumer<K, V> createConsumer(final String groupId,
                                               final Serde<K> keySerde,
                                               final Serde<V> valueSerde) {

      final Map<String, Object> props = getConsumerProps();
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

      LOGGER.debug(() ->
              "Creating consumer with properties:\n" + props.entrySet().stream()
                      .map(entry -> "    " + entry.getKey() + ": " + entry.getValue().toString())
                      .collect(Collectors.joining("\n"))
      );

      return new KafkaConsumer<>(
              props,
              keySerde.deserializer(),
              valueSerde.deserializer());
   }

   private Map<String, Object> getConsumerProps() {

      Map<String, Object> consumerProps = new HashMap<>();

      //ensure the session timeout is bigger than the flush timeout so our session doesn't expire
      //before we try to commit after a flush
      //Also ensure it isn't less than the default of 10s
      int sessionTimeoutMs = Math.max(10_000, (int) (getFlushIntervalMs() * 1.2));

      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
              stroomPropertyService.getPropertyOrThrow(StatisticsIngestService.PROP_KEY_KAFKA_BOOTSTRAP_SERVERS));
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getPollRecords());
      consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, getAutoOffsetReset());
//        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

      return consumerProps;
   }

   private int getFlushIntervalMs() {
      return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_MAX_FLUSH_INTERVAL_MS, 60_000);
   }

   private String getAutoOffsetReset() {
      return stroomPropertyService.getProperty(StatisticsIngestService.PROP_KEY_KAFKA_AUTO_OFFSET_RESET, "latest");
   }

   private int getPollRecords() {
      return stroomPropertyService.getIntProperty(PROP_KEY_AGGREGATOR_POLL_RECORDS, 1000);
   }

}
