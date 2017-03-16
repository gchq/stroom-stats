package stroom.stats.hbase.uid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.table.UniqueIdForwardMapTable;
import stroom.stats.hbase.table.UniqueIdReverseMapTable;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class UniqueIdProvider implements Provider<UniqueId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueIdProvider.class);

    private final UniqueId uniqueId;

    @Inject
    public UniqueIdProvider(final UniqueIdForwardMapTable uniqueIdForwardMapTable,
                            final UniqueIdReverseMapTable uniqueIdReverseMapTable) {

        this.uniqueId = new UniqueId(uniqueIdForwardMapTable, uniqueIdReverseMapTable, UID.UID_ARRAY_LENGTH);
    }


    /**
     * Provides a fully-constructed and injected instance of {@code T}.
     *
     * @throws RuntimeException if the injector encounters an error while
     *                          providing an instance. For example, if an injectable member on
     *                          {@code T} throws an exception, the injector may wrap the exception
     *                          and throw it to the caller of {@code get()}. Callers should not try
     *                          to handle such exceptions as the behavior may vary across injector
     *                          implementations and even different configurations of the same injector.
     */
    @Override
    public UniqueId get() {
        return uniqueId;
    }
}
