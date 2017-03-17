

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

package stroom.stats.shared.hbase.monitoring;


import stroom.stats.configuration.common.SharedObject;

public class EventStoreAggregatorStatus implements SharedObject, Comparable<EventStoreAggregatorStatus> {
    private static final long serialVersionUID = -7362207138336313048L;

    private String node;
    private int orderNo;
    private String name;
    private String storeInterval;
    private Long threadId;
    private String statisticType;
    private String value;

    public EventStoreAggregatorStatus() {
        // Default constructor necessary for GWT serialisation.
    }

    public EventStoreAggregatorStatus(final String node, final Integer orderNo, final String name,
            final String storeInterval, final Long threadId, final String statisticType, final String value) {
        this.node = node;
        this.orderNo = orderNo;
        this.name = name;
        this.storeInterval = storeInterval;
        this.threadId = threadId;
        this.statisticType = statisticType;
        this.value = value;
    }

    public String getNode() {
        return node;
    }

    public void setNode(final String node) {
        this.node = node;
    }

    public int getOrderNo() {
        return orderNo;
    }

    public void setOrderNo(final int orderNo) {
        this.orderNo = orderNo;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public String getStoreInterval() {
        return storeInterval;
    }

    public void setStoreInterval(final String storeInterval) {
        this.storeInterval = storeInterval;
    }

    public Long getThreadId() {
        return threadId;
    }

    public void setThreadId(final Long threadId) {
        this.threadId = threadId;
    }

    public String getStatisticType() {
        return statisticType;
    }

    public void setStatisticType(final String statisticType) {
        this.statisticType = statisticType;
    }

    @Override
    public String toString() {
        return "EventStoreAggregatorStatus [node=" + node + ", name=" + name + ", storeInterval=" + storeInterval
                + ", threadId=" + threadId + ", statisticType=" + statisticType + ", value=" + value + "]";
    }

    private String getCompareString() {
        final StringBuilder sb = new StringBuilder();

        final String separator = "~#~";

        sb.append(node);
        sb.append(separator);
        sb.append(orderNo);
        sb.append(separator);
        sb.append(name);
        sb.append(separator);
        sb.append(statisticType);
        sb.append(separator);
        sb.append(storeInterval);
        sb.append(separator);
        sb.append(threadId);
        sb.append(separator);

        return sb.toString();
    }

    @Override
    public int compareTo(final EventStoreAggregatorStatus other) {
        return getCompareString().compareTo(other.getCompareString());
    }

}
