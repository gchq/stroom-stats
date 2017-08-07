package stroom.stats.test;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Properties;

public class KafkaEmbededUtils {

    private KafkaEmbededUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEmbededUtils.class);

    public static void createTopics(final KafkaEmbedded kafkaEmbedded, final String... topics) {

        ZkUtils zkUtils = new ZkUtils(kafkaEmbedded.getZkClient(), null, false);
        for (String topic : topics) {
            LOGGER.info("Creating topic {}", topic);
            AdminUtils.createTopic(zkUtils,
                    topic,
                    kafkaEmbedded.getPartitionsPerTopic(),
                    kafkaEmbedded.getBrokerAddresses().length,
                    new Properties(),
                    null);
        }
    }

    public static void deleteTopics(final KafkaEmbedded kafkaEmbedded, final String... topics) {
        ZkUtils zkUtils = new ZkUtils(kafkaEmbedded.getZkClient(), null, false);
        for (String topic : topics) {

            if (AdminUtils.topicExists(zkUtils, topic)) {
                LOGGER.info("Deleting topic {}", topic);
                kafkaEmbedded.waitUntilSynced(topic, 0);
                AdminUtils.deleteTopic(zkUtils, topic);
                while (AdminUtils.topicExists(zkUtils, topic)) {
                    //wait for topic to die
                }
            }
        }
    }

}
