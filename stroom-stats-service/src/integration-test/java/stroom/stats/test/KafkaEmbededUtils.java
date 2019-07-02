package stroom.stats.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.Arrays;

public class KafkaEmbededUtils {

    private KafkaEmbededUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEmbededUtils.class);

    public static void deleteAndCreateTopics(final EmbeddedKafkaBroker kafkaEmbedded, final String... topics) {
        //attempt deletion first to ensure the topics don't already exist
        deleteTopics(kafkaEmbedded, topics);
        createTopics(kafkaEmbedded, topics);
    }
    public static void createTopics(final EmbeddedKafkaBroker kafkaEmbedded, final String... topics) {

//        ZkUtils zkUtils = new ZkUtils(kafkaEmbedded.getZkClient(), null, false);

        LOGGER.info("Creating topics {}", topics);
        kafkaEmbedded.addTopics(topics);

//        for (String topic : topics) {
//            AdminUtils.createTopic(zkUtils,
//                    topic,
//                    kafkaEmbedded.getPartitionsPerTopic(),
//                    kafkaEmbedded.getBrokerAddresses().length,
//                    new Properties(),
//                    null);
//        }
    }

    public static void deleteTopics(final EmbeddedKafkaBroker kafkaEmbedded, final String... topics) {
//        ZkUtils zkUtils = new ZkUtils(kafkaEmbedded.getZkClient(), null, false);
        kafkaEmbedded.doWithAdmin(adminClient -> {
            LOGGER.info("Deleting topic {}", topics);
            adminClient.deleteTopics(Arrays.asList(topics));
        });
//        for (String topic : topics) {
//
//            if (AdminUtils.topicExists(zkUtils, topic)) {
//                kafkaEmbedded.waitUntilSynced(topic, 0);
//                AdminUtils.deleteTopic(zkUtils, topic);
//                while (AdminUtils.topicExists(zkUtils, topic)) {
//                    //wait for topic to die
//                }
//                kafkaEmbedded.waitUntilSynced(topic, 0);
//            }
//        }
    }

}
