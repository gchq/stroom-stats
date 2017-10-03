package stroom.stats.hbase;

import com.google.inject.Injector;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.AbstractAppIT;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;

public class UniqueIdCacheImplIT extends AbstractAppIT {

    private final Injector injector = getApp().getInjector();
    private final UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);

    @Test
    public void testGetOrCreateId() {

        String uuid = "myUUID";
        UID uid = uniqueIdCache.getOrCreateId(uuid);

        Assertions.assertThat(uid).isNotNull();

        String uuid2 = uniqueIdCache.getName(uid);

        Assertions.assertThat(uuid).isEqualTo(uuid2);
    }
}
