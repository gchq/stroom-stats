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

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class ZookeeperConfig {

    @NotNull
    @JsonProperty
    private String quorum;

    @NotNull
    @JsonProperty
    private String rootPath;

    @NotNull
    @JsonProperty
    private String propertyServicePath;

    @NotNull
    @Min(0)
    @Max(Integer.MAX_VALUE)
    @JsonProperty
    private int propertyServiceTreeCacheTimeoutMs;

    public String getQuorum() {
        return quorum;
    }

    public String getRootPath() {
        return rootPath;
    }

    public String getPropertyServicePath() {
        return propertyServicePath;
    }

    public int getPropertyServiceTreeCacheTimeoutMs() {
        return propertyServiceTreeCacheTimeoutMs;
    }

    @Override
    public String toString() {
        return "ZookeeperConfig{" +
                "quorum='" + quorum + '\'' +
                ", rootPath='" + rootPath + '\'' +
                ", propertyServicePath='" + propertyServicePath + '\'' +
                ", propertyServiceTreeCacheTimeoutMs=" + propertyServiceTreeCacheTimeoutMs +
                '}';
    }
}
