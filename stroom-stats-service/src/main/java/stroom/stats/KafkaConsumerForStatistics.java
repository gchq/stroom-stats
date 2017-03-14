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
 */

package stroom.stats;

import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.config.Config;
import stroom.stats.schema.Statistics;

import javax.inject.Singleton;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.util.concurrent.ExecutorService;

@Singleton
@Deprecated //replaced by kafka streams, left here pending borrowing some of the health check code
public class KafkaConsumerForStatistics {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerForStatistics.class);

    private ExecutorService consumerExecutor;
    private final Config config;
    private final HBaseClient hBaseClient;
    private ServiceDiscoveryManager services;
    private final JAXBContext jaxbContext;
    private int countOfFailedDeserialisations = 0;
    private int countOfAllMessages = 0;
    private boolean ableToAccessKafka = true;

//    @Inject
    public KafkaConsumerForStatistics(Config config, HBaseClient hBaseClient, ServiceDiscoveryManager services) throws Exception {
        this.config = config;
        this.hBaseClient = hBaseClient;
        this.services = services;
        LOGGER.info("KafkaConsumerForStatistics has started.");
        jaxbContext = JAXBContext.newInstance(Statistics.class);
    }

//    @Inject
    public void startReceiving() {
        LOGGER.info("About to start consuming statistics.");
//        consumerExecutor = Executors.newSingleThreadExecutor();
//        consumerExecutor.submit(() -> {
//            try {
//                ServiceInstance<String> kafka = services.getKafka();

//                SimpleKafkaConsumer<String> consumer = SimpleKafkaConsumer.newBuilder()
//                        .kafkaHost(kafka.getAddress() + ":" + kafka.getPort())
//                        .groupId(config.getKafkaConfig().getGroupId())
//                        .keyDeserialiser((StringDeserializer.class.getName()))
//                        .valueDeserialiser((StringDeserializer.class.getName()))
//                        .build();

//                consumer.startReceiving(config.getKafkaConfig().getStatisticsTopic(), this::handleNewMessage);
//            } catch (Exception e) {
//                LOGGER.error("Unable to access kafka!", e);
//                ableToAccessKafka = false;
//            }
//        });
    }

    public void stop() {
        consumerExecutor.shutdownNow();
    }

    public String getName() {
        return "message-consumer";
    }

    void handleNewMessage(String message) {
        LOGGER.debug("Received message: {}", message);
        countOfAllMessages++;

        try {
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            Statistics statistics = (Statistics) unmarshaller.unmarshal(new StringReader(message));
            hBaseClient.addStatistics(statistics);
        } catch (JAXBException e) {
            int trimIndex = message.length() < 50 ? message.length() : 49;
            LOGGER.error("Unable to deserialise a message (enable debug to log full message): {}...", message.substring(0, trimIndex));
            LOGGER.debug("Unable to deserialise a message {}", message);
            countOfFailedDeserialisations++;
        }
    }

    public HealthCheck.Result getHealth(){
        if(ableToAccessKafka) {
            if (countOfAllMessages == 0) {
                return HealthCheck.Result.healthy("I have not yet received any messages.");
            } else if (countOfFailedDeserialisations > 0) {
                return HealthCheck.Result.unhealthy(
                        String.format("I have been unable to deserialise %s out of %s messages!",
                                countOfFailedDeserialisations,
                                countOfAllMessages));
            } else {
                return HealthCheck.Result.healthy(
                        String.format("I have successfully handled all %s messages.", countOfAllMessages));
            }
        }
        else {
            return HealthCheck.Result.unhealthy("Unable to access Kafka!");
        }
    }

}
