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

package stroom.stats.streams.serde;

import org.apache.kafka.common.serialization.Serde;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.MultiPartIdentifier;
import stroom.stats.streams.aggregation.StatAggregate;
import stroom.stats.streams.aggregation.ValueAggregate;

import java.util.ArrayList;
import java.util.List;

public class TestValueAggregateSerde {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestValueAggregateSerde.class);

    @Test
    public void serializeDeserialize() throws Exception {

        List<MultiPartIdentifier> identifiers = new ArrayList<>();
        identifiers.add(new MultiPartIdentifier("StringId1", 123L));
        identifiers.add(new MultiPartIdentifier("StringId2", 456L));
        ValueAggregate valueAggregate = new ValueAggregate(identifiers, 10, 7.89);

        Serde<StatAggregate> valueAggregateSerde = StatAggregateSerde.instance();

        SerdeUtils.verify(valueAggregateSerde, valueAggregate);

        //verify will throw an exception if the two objects don't match
    }

}