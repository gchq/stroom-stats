package stroom.stats.hbase.uid;

import com.google.inject.Injector;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.AbstractAppIT;

import java.util.Optional;
import java.util.UUID;

public class UniqueIdCacheImplIT extends AbstractAppIT {

    private final Injector injector = getApp().getInjector();
    private final UniqueIdCache uniqueIdCache = injector.getInstance(UniqueIdCache.class);

    @Test
    public void testGetOrCreateId() {

        String name = "myName" + UUID.randomUUID().toString();

        //name should be created in hbase
        UID uid = uniqueIdCache.getOrCreateId(name);

        Assertions.assertThat(uid).isNotNull();

        //should have a reverse map in hbase
        String name2 = uniqueIdCache.getName(uid);

        Assertions.assertThat(name).isEqualTo(name2);

        //go full circle
        UID uid2 = uniqueIdCache.getUniqueId(name).get();

        Assertions.assertThat(uid).isEqualTo(uid2);
    }

    @Test
    public void testUnknownNameNotCached() {

        String name = "myName" + UUID.randomUUID().toString();

        Optional<UID> optUid = uniqueIdCache.getUniqueId(name);

        Assertions.assertThat(optUid).isEmpty();

        //now try a getOrCreate call, which should go past the cache to create a new UID
        UID uid = uniqueIdCache.getOrCreateId(name);

        Assertions.assertThat(uid).isNotNull();
    }

    @Test(expected = RuntimeException.class)
    public void testGetName_unknownName() {

        UID unknownUid = UID.NOT_FOUND_UID;

        //will throw an exception as we should never have a name without a corresponding uid
        String name = uniqueIdCache.getName(unknownUid);
    }

    @Test
    public void testGetUniqueIdOrDefault() {

        String uuid = "unknownUuid";
        UID uid = uniqueIdCache.getUniqueIdOrDefault(uuid);

        Assertions.assertThat(uid).isEqualTo(UID.NOT_FOUND_UID);
    }

    @Test
    public void testGetUniqueId_notKnown() {

        String unknownUuid = UUID.randomUUID().toString();
        Optional<UID> optUid = uniqueIdCache.getUniqueId(unknownUuid);
        Assertions.assertThat(optUid.isPresent()).isFalse();
    }
}
