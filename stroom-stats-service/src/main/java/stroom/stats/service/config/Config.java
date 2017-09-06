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

package stroom.stats.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.spinscale.dropwizard.jobs.JobConfiguration;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public class Config extends Configuration implements JobConfiguration {

    @NotNull
    @Valid
    private String jwtTokenSecret;

    @Valid
    private Optional<String> advertisedHostNameOrIp;

    @Valid
    private OptionalInt advertisedPort;

    @NotNull
    @Valid
    @JsonProperty("zookeeper")
    private ZookeeperConfig zookeeperConfig;

    @NotNull
    @Valid
    @JsonProperty("defaultProperties")
    private Map<String,String> defaultProperties;

    @NotNull
    @Valid
    @JsonProperty
    private DataSourceFactory database = new DataSourceFactory();

    public DataSourceFactory getDataSourceFactory() {
        return database;
    }

    public byte[] getJwtTokenSecret() {
        return jwtTokenSecret.getBytes(Charset.defaultCharset());
    }

    public Optional<String> getAdvertisedHostNameOrIp() {
        return advertisedHostNameOrIp;
    }

    public OptionalInt getAdvertisedPort() {
        return advertisedPort;
    }

    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    public Map<String, String> getDefaultProperties() {
        return defaultProperties;
    }

    @Override
    public String toString() {
        return "Config{" +
                "jwtTokenSecret='" + jwtTokenSecret + '\'' +
                ", advertisedHostNameOrIpAddress='" + advertisedHostNameOrIp + '\'' +
                ", advertisedPort='" + advertisedPort + '\'' +
                ", zookeeperConfig=" + zookeeperConfig +
                ", defaultProperties=" + defaultProperties +
                ", database=" + database +
                '}';
    }
}

