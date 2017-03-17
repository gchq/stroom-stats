/*
 * Copyright 2017 Crown Copyright
 *
 * This file is part of Stroom-Stats.
 *
 * Stroom-Stats is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stroom-Stats is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stroom-Stats.  If not, see <http://www.gnu.org/licenses/>.
 */

package stroom.stats.hbase;

import com.google.inject.Injector;
import org.junit.Test;
import stroom.stats.AbstractAppIT;
import stroom.stats.hbase.uid.HBaseUniqueIdGenerator;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdGenerator;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class UniqueIdIT extends AbstractAppIT {

    @Test
    public void testGetOrCreateId() {
        Injector injector = getApp().getInjector();
        UniqueIdGenerator uniqueIdGenerator = injector.getInstance(HBaseUniqueIdGenerator.class);

        String statNameStr = this.getClass().getName() + "-testGetOrCreateId-" + Instant.now().toString();
        //get the id for a name that will not exist, thus creating the mapping
        byte[] id = uniqueIdGenerator.getOrCreateId(statNameStr);

        assertThat(id).isNotNull();
        assertThat(id).hasSize(UID.UID_ARRAY_LENGTH);

        Optional<String> optName = uniqueIdGenerator.getName(id);

        //ensure the reverse map is also present
        assertThat(optName).hasValue(statNameStr);

        //now get the id for the same string which was created above
        byte[] id2 = uniqueIdGenerator.getOrCreateId(statNameStr);

        assertThat(id2).isEqualTo(id);

        //now get the id for the same string using getId
        Optional<byte[]> id3 = uniqueIdGenerator.getId(statNameStr);

        assertThat(id3).hasValue(id);
    }

    @Test
    public void testGetId_notExists() {
        Injector injector = getApp().getInjector();
        UniqueIdGenerator uniqueIdGenerator = injector.getInstance(HBaseUniqueIdGenerator.class);

        //try and get an id for a name that will not exist
        String statNameStr = this.getClass().getName() + "-testGetId-" + Instant.now().toString();
        Optional<byte[]> optId = uniqueIdGenerator.getId(statNameStr);

        assertThat(optId).isEmpty();
    }
}
