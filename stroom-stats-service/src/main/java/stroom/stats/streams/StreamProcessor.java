package stroom.stats.streams;

import org.apache.kafka.streams.Topology;
import stroom.stats.util.HasName;
import stroom.stats.util.Startable;
import stroom.stats.util.Stoppable;

import java.util.Properties;

public interface StreamProcessor extends Stoppable, Startable, HasName {

    default String getName() {
        return this.getClass().getSimpleName();
    }

    Topology getTopology();

    Properties getStreamConfig();

    default String getAppId() {
        return getName();
    }
}
