

/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
 */

package stroom.stats.hbase.uid;

import javaslang.control.Try;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.hbase.util.bytes.UnsignedBytes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * Simple {@link UniqueIdCache} implementation using basic hashmaps to save
 * having to use EHCache
 */
public class MockUniqueIdCache implements UniqueIdCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockUniqueIdCache.class);

    private final AtomicInteger idSequence = new AtomicInteger(0);

    private final ConcurrentMap<UID, String> idToNameMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UID> nameToIdMap = new ConcurrentHashMap<>();

    @Override
    public UID getOrCreateId(final String name) {
        final UID uniqueId = nameToIdMap.computeIfAbsent(name, nameValue -> {
            // name not mapped so create a new ID
            final byte[] id = generateNewId();

            final UID uid = UID.from(id);

            LOGGER.trace("Creating mock UID: {} for name: {} ", uid.toString(), nameValue);

            idToNameMap.put(uid, nameValue);

            return uid;
        });

        return uniqueId;
    }

    @Override
    public Try<UID> getUniqueId(final String name) {
        return Try.of(() -> nameToIdMap.get(name));
    }

    @Override
    public String getName(final UID id) {
        return idToNameMap.get(id);
    }

    @Override
    public int getWidth() {
        return UID.UID_ARRAY_LENGTH;
    }

    @Override
    public int getCacheSize() {
        return nameToIdMap.size();
    }

    private byte[] generateNewId() {
        return convertToUid(idSequence.incrementAndGet(), UID.UID_ARRAY_LENGTH);
    }


    public static byte[] convertToUid(final long id, final int width) {
        final byte[] uid = new byte[width];

        UnsignedBytes.put(uid, 0, width, id);

        return uid;
    }

    // some basic tests to ensure the mock is working so we can rely on it in
    // other test classes

    @Test
    public void testGetOrCreateId() {
        final String name = "myCountStat";
        final UID uniqueId = getOrCreateId(name);

        assertEquals(1, idToNameMap.size());
        assertEquals(1, nameToIdMap.size());

        assertEquals(name, getName(uniqueId));

        final String name2 = "myCountStat";
        final UID uniqueId2 = getOrCreateId(name2);

        assertEquals(uniqueId, uniqueId2);
    }
}
