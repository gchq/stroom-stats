package stroom.stats;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class HostAvailabilityIT {

    public static final String HBASE_HOSTNAME = "stroom.hbase";
    public static final int HBASE_PORT = 60000;
    public static final String KAFKA_HOSTNAME = "stroom.kafka";
    public static final int KAFKA_PORT = 9092;

    @Test
    public void hbaseServerAvailability() {
        Assert.assertTrue("HBase is unavailable. Is the hostname in /etc/hosts?", pingHost(HBASE_HOSTNAME, HBASE_PORT, 500));
    }

    @Test
    public void kafkaServerAvailability() {
        Assert.assertTrue("Kafka is unavailable. Is the hostname in /etc/hosts?", pingHost(KAFKA_HOSTNAME, KAFKA_PORT, 500));
    }

    public static boolean pingHost(String host, int port, int timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeout);
            return true;
        } catch (IOException e) {
            return false; // Either timeout or unreachable or failed DNS lookup.
        }
    }
}
