

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

package stroom.stats.hbase.table.filter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import org.objenesis.strategy.StdInstantiatorStrategy;
import stroom.stats.hbase.structure.RowKeyTagValue;
import stroom.stats.hbase.structure.TagValueFilterTreeNode;
import stroom.stats.hbase.uid.UID;
import stroom.stats.util.logging.LambdaLogger;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public class TagValueFilterTreeSerialiser {

    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(TagValueFilterTreeSerialiser.class);

    private static final KryoFactory factory = () -> {
        Kryo kryo = new Kryo();
        try {
            LOGGER.debug(() -> String.format("Initialising Kryo on thread %s",
                    Thread.currentThread().getName()));

            kryo.register(TagValueFilterTree.class);
            kryo.register(TagValueFilterTreeNode.class);
            kryo.register(TagValueOperatorNode.class);
            kryo.register(FilterOperationMode.class);
            kryo.register(List.class);
            kryo.register(ArrayList.class);
            kryo.register(RowKeyTagValue.class);
            kryo.register(UID.class);
            kryo.register(byte[].class);
            kryo.register(Object.class);
            kryo.register(Integer.class);

            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(
                    new StdInstantiatorStrategy());
            kryo.setRegistrationRequired(true);
        } catch (Exception e) {
            LOGGER.error("Exception occurred configuring kryo instance", e);
        }
        return kryo;
    };

    private static final KryoPool pool = new KryoPool.Builder(factory)
            .softReferences()
            .build();

    private TagValueFilterTreeSerialiser() {
    }

    public static TagValueFilterTreeSerialiser instance() {
        return new TagValueFilterTreeSerialiser();
    }

    public byte[] serialize(TagValueFilterTree tagValueFilterTree) {
        return pool.run(kryo -> {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Output output = new Output(stream);
            kryo.writeClassAndObject(output, tagValueFilterTree);
            output.close();
            return stream.toByteArray();
        });
    }

    public TagValueFilterTree deserialize(byte[] bytes) {
        return pool.run(kryo -> {
            Input input = new Input(bytes);
            return (TagValueFilterTree) kryo.readClassAndObject(input);
        });
    }
}
