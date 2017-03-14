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
 *
 *
 */

package stroom.stats.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.spinscale.dropwizard.jobs.JobConfiguration;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public class Config extends Configuration implements JobConfiguration {
    //TODO add all the stroom hbase properties in here

    //TODO need to figure out what to do about cluster wide properties
    //when we have multiple dropwiz instances, e.g. the ZK quorum prop.
    //Maybe we just congure each instance individually and ensure that
    //each instance has the correct values.

    @NotNull
    @Valid
    private String jwtTokenSecret;

    @NotNull
    @Valid
    @JsonProperty("kafka")
    private KafkaConfig kafkaConfig;

    @NotNull
    @Valid
    @JsonProperty("zookeeper")
    private ZookeeperConfig zookeeperConfig;

    @NotNull
    @Valid
    @JsonProperty
    private DataSourceFactory database = new DataSourceFactory();

    public DataSourceFactory getDataSourceFactory() {
        return database;
    }

    public byte[] getJwtTokenSecret() throws UnsupportedEncodingException {
        return jwtTokenSecret.getBytes(Charset.defaultCharset());
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    @Override
    public String toString() {
        return "Config{" +
                "jwtTokenSecret='" + jwtTokenSecret + '\'' +
                ", kafkaConfig=" + kafkaConfig +
                ", zookeeperConfig=" + zookeeperConfig +
                ", database=" + database +
                '}';
    }
}

