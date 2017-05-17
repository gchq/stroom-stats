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

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class ZookeeperConfig {

    @NotNull
    @JsonProperty
    private String quorum;

    @NotNull
    @JsonProperty
    private String statsPath;

    @NotNull
    @JsonProperty
    private String propertyServicePath;

    @NotNull
    @Min(0)
    @Max(Integer.MAX_VALUE)
    @JsonProperty
    private int propertyServiceTreeCacheTimeoutMs;

    @NotNull
    @JsonProperty
    private String serviceDiscoveryPath;

    public String getQuorum() {
        return quorum;
    }

    public String getStatsPath() {
        return statsPath;
    }

    public String getPropertyServicePath() {
        return propertyServicePath;
    }

    public int getPropertyServiceTreeCacheTimeoutMs() {
        return propertyServiceTreeCacheTimeoutMs;
    }

    public String getServiceDiscoveryPath() {
        return serviceDiscoveryPath;
    }

    @Override
    public String toString() {
        return "ZookeeperConfig{" +
                "quorum='" + quorum + '\'' +
                ", statsPath='" + statsPath + '\'' +
                ", propertyServicePath='" + propertyServicePath + '\'' +
                ", propertyServiceTreeCacheTimeoutMs=" + propertyServiceTreeCacheTimeoutMs +
                ", serviceDiscoveryPath=" + serviceDiscoveryPath +
                '}';
    }
}
