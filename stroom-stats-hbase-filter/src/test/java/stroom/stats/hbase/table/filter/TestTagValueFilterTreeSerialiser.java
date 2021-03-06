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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import stroom.stats.hbase.structure.RowKeyTagValue;
import stroom.stats.hbase.structure.TagValueFilterTreeNode;
import stroom.stats.hbase.uid.UID;

import java.util.Arrays;

public class TestTagValueFilterTreeSerialiser {

    @Test
    public void serializeDeserialize() throws Exception {
        UID tag1 = UID.from(new byte[]{0, 0, 0, 1});
        UID value1 = UID.from(new byte[]{0, 0, 1, 1});
        UID tag2 = UID.from(new byte[]{0, 0, 0, 2});
        UID value2 = UID.from(new byte[]{0, 0, 1, 2});

        TagValueFilterTreeNode tagNode1 = new RowKeyTagValue(tag1, value1);
        TagValueFilterTreeNode tagNode2 = new RowKeyTagValue(tag2, value2);
        TagValueFilterTreeNode rootNode = new TagValueOperatorNode(FilterOperationMode.AND, Arrays.asList(tagNode1, tagNode2));

        TagValueFilterTree tagValueFilterTree = new TagValueFilterTree(rootNode);

        TagValueFilterTreeSerialiser serialiser = TagValueFilterTreeSerialiser.instance();

        byte[] bytes = serialiser.serialize(tagValueFilterTree);

        TagValueFilterTree tagValueFilterTree2 = serialiser.deserialize(bytes);

        byte[] bytes2 = serialiser.serialize(tagValueFilterTree2);

        Assertions.assertThat(tagValueFilterTree).isEqualTo(tagValueFilterTree2);
        Assertions.assertThat(bytes).isEqualTo(bytes2);

    }

    @Test
    public void serializeDeserialize_emptyTree() throws Exception {

        TagValueFilterTree tagValueFilterTree = TagValueFilterTree.emptyTree();

        TagValueFilterTreeSerialiser serialiser = TagValueFilterTreeSerialiser.instance();

        byte[] bytes = serialiser.serialize(tagValueFilterTree);

        TagValueFilterTree tagValueFilterTree2 = serialiser.deserialize(bytes);

        byte[] bytes2 = serialiser.serialize(tagValueFilterTree2);

        Assertions.assertThat(tagValueFilterTree).isEqualTo(tagValueFilterTree2);
        Assertions.assertThat(bytes).isEqualTo(bytes2);

    }

}