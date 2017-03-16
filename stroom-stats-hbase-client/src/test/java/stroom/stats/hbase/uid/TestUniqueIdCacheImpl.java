package stroom.stats.hbase.uid;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.cache.CacheFactory;
import stroom.stats.hbase.table.UniqueIdForwardMapTable;
import stroom.stats.hbase.table.UniqueIdReverseMapTable;
import stroom.stats.hbase.util.bytes.UnsignedBytes;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestUniqueIdCacheImpl {

    @Rule
    private MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private CacheFactory mockCacheFactory;

    @Test
    public void getOrCreateId() throws Exception {

//        when(mockCacheFactory.getOrCreateCache(argThat(matches(UniqueIdCacheImpl.NAME_TO_UID_CACHE_NAME)))
//        UniqueIdCache uniqueIdCache = new UniqueIdCacheImpl(
//                new MockUniqueId(),
//
//        )


    }

    @Test
    public void getUniqueId() throws Exception {

    }

    @Test
    public void getName() throws Exception {

    }

    //partial mock
    private static class MockUniqueId extends UniqueId {

        private static final Logger LOGGER = LoggerFactory.getLogger(MockUniqueId.class);

        private final AtomicInteger idSequence = new AtomicInteger(0);
        private final ConcurrentMap<UID, String> idToNameMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, byte[]> nameToIdMap = new ConcurrentHashMap<>();

        public MockUniqueId() {
            super(null, null, UID.UID_ARRAY_LENGTH);
        }

        @Override
        public Optional<byte[]> getId(String name) throws IOException {
            return Optional.ofNullable(nameToIdMap.get(name));
        }

        @Override
        public byte[] getOrCreateId(String name) {
            final byte[] uniqueId = nameToIdMap.computeIfAbsent(name, key -> {
                // name not mapped so create a new ID
                final byte[] id = generateNewId();

                final UID uid = UID.from(id);

                LOGGER.trace("Creating mock UID: {} for name: {} ", uid.toString(), key);

                idToNameMap.put(uid, key);

                return id;
            });

            return uniqueId;
        }

        @Override
        public Optional<String> getName(byte[] id) {
            return Optional.ofNullable(idToNameMap.get(UID.from(id)));
        }

        private byte[] generateNewId() {
            return convertToUid(idSequence.incrementAndGet(), UID.UID_ARRAY_LENGTH);
        }


        public static byte[] convertToUid(final long id, final int width) {
            final byte[] uid = new byte[width];

            UnsignedBytes.put(uid, 0, width, id);

            return uid;
        }
    }

}