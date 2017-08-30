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

package stroom.stats;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class HostAvailabilityIT {

    public static final String HBASE_HOSTNAME = "hbase";
    public static final int HBASE_PORT = 60000;
    public static final String KAFKA_HOSTNAME = "kafka";
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
