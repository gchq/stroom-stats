package stroom.stats.correlation;

import java.util.Collection;

public class BasicCorrelator<T> extends AbstractCorrelator<Collection<T>, T> {

    public BasicCorrelator() {
        //Basic correlator has no container object so just return itself in both cases
        super(obj -> obj, collection -> collection);
    }
}
