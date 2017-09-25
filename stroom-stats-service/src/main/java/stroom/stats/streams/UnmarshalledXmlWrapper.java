package stroom.stats.streams;

import com.google.common.base.Preconditions;
import stroom.stats.schema.v4.Statistics;

/**
 * Wrapper class to hold the results of unmarshalling an XML string. It will either contain a valid unmarshalled object
 * or in the case of a failure to unmarshall, contain the original XML string along with the exception throw during
 * unmarshalling
 */
public class UnmarshalledXmlWrapper implements HasValidity {

    private final Statistics statistics;
    private final String messageValue;
    private final Throwable throwable;

    private UnmarshalledXmlWrapper(final Statistics statistics, final String messageValue, final Throwable throwable) {
        this.statistics = statistics;
        this.messageValue = messageValue;
        this.throwable = throwable;
    }

    public static UnmarshalledXmlWrapper wrapValidMessage(final Statistics statistics) {
        Preconditions.checkNotNull(statistics);
        return new UnmarshalledXmlWrapper(statistics, null, null);
    }

    public static UnmarshalledXmlWrapper wrapInvalidMessage(final String messageValue, final Throwable throwable) {
        Preconditions.checkNotNull(throwable);
        return new UnmarshalledXmlWrapper(null, messageValue == null ? "" : messageValue, throwable);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public String getMessageValue() {
        return messageValue;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public boolean isValid() {
        return statistics != null;
    }
}
