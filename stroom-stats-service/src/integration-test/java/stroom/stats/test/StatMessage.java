package stroom.stats.test;

import stroom.stats.schema.v4.Statistics;

public class StatMessage {
    private final String topic;
    private final String key;
    private final Statistics statistics;

    public StatMessage(final String topic, final String key, final Statistics statistics) {
        this.topic = topic;
        this.key = key;
        this.statistics = statistics;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public Statistics getStatistics() {
        return statistics;
    }
}
