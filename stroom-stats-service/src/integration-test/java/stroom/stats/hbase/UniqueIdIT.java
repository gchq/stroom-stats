package stroom.stats.hbase;

import com.google.inject.Injector;
import org.junit.Test;
import stroom.stats.AbstractAppIT;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueId;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class UniqueIdIT extends AbstractAppIT {

    @Test
    public void testGetOrCreateId() {
        Injector injector = getApp().getInjector();
        UniqueId uniqueId = injector.getInstance(UniqueId.class);

        String statNameStr = this.getClass().getName() + "-testGetOrCreateId-" + Instant.now().toString();
        //get the id for a name that will not exist, thus creating the mapping
        byte[] id = uniqueId.getOrCreateId(statNameStr);

        assertThat(id).isNotNull();
        assertThat(id).hasSize(UID.UID_ARRAY_LENGTH);

        Optional<String> optName = uniqueId.getName(id);

        //ensure the reverse map is also present
        assertThat(optName).hasValue(statNameStr);

        //now get the id for the same string which was created above
        byte[] id2 = uniqueId.getOrCreateId(statNameStr);

        assertThat(id2).isEqualTo(id);

        //now get the id for the same string using getId
        Optional<byte[]> id3 = uniqueId.getId(statNameStr);

        assertThat(id3).hasValue(id);
    }

    @Test
    public void testGetId_notExists() {
        Injector injector = getApp().getInjector();
        UniqueId uniqueId = injector.getInstance(UniqueId.class);

        //try and get an id for a name that will not exist
        String statNameStr = this.getClass().getName() + "-testGetId-" + Instant.now().toString();
        Optional<byte[]> optId = uniqueId.getId(statNameStr);

        assertThat(optId).isEmpty();
    }
}
