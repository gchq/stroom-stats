package stroom.stats.hbase.uid;

import com.google.inject.Injector;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.AbstractAppIT;

import java.util.UUID;

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


        UID uid2 = uniqueIdCache.getUniqueId(uuid).get();

        Assertions.assertThat(uid).isEqualTo(uid2);
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
        boolean isFailure = uniqueIdCache.getUniqueId(unknownUuid).isFailure();
        Assertions.assertThat(isFailure).isTrue();

    }
}
