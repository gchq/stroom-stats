package stroom.stats.test;

public class BadStatMessage {
    private final String topic;
    private final String key;
    private final String value;

    public BadStatMessage(final String topic, final String key, final String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
