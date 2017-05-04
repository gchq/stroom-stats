package stroom.stats.correlation;

import java.util.Collection;

public class BasicCorrelator<E> extends AbstractCorrelator<Collection<E>, E> {

    public BasicCorrelator() {
        //Basic correlator has no container object so just return itself in both cases
        super(obj -> obj, collection -> collection);
    }
}
