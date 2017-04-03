

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

import stroom.stats.api.StatisticTag;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.FilterTermsTree.TermNode;
import stroom.stats.hbase.structure.RowKeyTagValue;
import stroom.stats.hbase.structure.TagValueFilterTreeNode;
import stroom.stats.hbase.uid.UID;
import stroom.stats.hbase.uid.UniqueIdCache;

import java.util.ArrayList;
import java.util.List;

public class TagValueFilterTreeBuilder {
    private TagValueFilterTreeBuilder() {
        // should never be instantiated as has only static methods
    }

    /**
     * Converts a {@link FilterTermsTree} into a {@link TagValueFilterTree}. The
     * plain text tags and values are converted into UIDs and
     * {@link RowKeyTagValue} objects replace the {@link TermNode} objects.
     *
     * @param filterTermsTree
     *            The tree to convert
     * @param uniqueIdCache
     *            A reference to the cache that can convert Strings into UIDs.
     * @return a new {@link TagValueFilterTree} instance
     */
    public static TagValueFilterTree buildTagValueFilterTree(final FilterTermsTree filterTermsTree,
            final UniqueIdCache uniqueIdCache) {
        TagValueFilterTree tagValueFilterTree;

        if (filterTermsTree == null || filterTermsTree.getRootNode() == null) {
            tagValueFilterTree = TagValueFilterTree.emptyTree();
        } else {
            final TagValueFilterTreeNode newRootNode = convertNode(filterTermsTree.getRootNode(),
                    uniqueIdCache);

            tagValueFilterTree = new TagValueFilterTree(newRootNode);
        }

        return tagValueFilterTree;
    }

    private static TagValueFilterTreeNode convertNode(final FilterTermsTree.Node oldNode,
            final UniqueIdCache uniqueIdCache) {
        if (oldNode instanceof TermNode) {
            return convertTermNode((TermNode) oldNode, uniqueIdCache);
        } else if (oldNode instanceof OperatorNode) {
            return convertOperatorNode((OperatorNode) oldNode, uniqueIdCache);
        } else {
            throw new RuntimeException(
                    "Node is of a type that we don't expect: " + oldNode.getClass().getCanonicalName());
        }
    }

    private static RowKeyTagValue convertTermNode(final TermNode oldNode, final UniqueIdCache uniqueIdCache) {
        String valueString = oldNode.getValue();

        if (valueString == null || valueString.isEmpty()) {
            valueString = StatisticTag.NULL_VALUE_STRING;
        }

        // lookup the tag and value in the UID cache
        // if the string is not mapped in the UID store then it will return the
        // UID_NOT_FOUND byte array
        // so depending on operator and position in the tree it may mean the
        // filter won't return anything
        // but we still have to do the search as this term could be part of a
        // NOT for example.
        // Same applies for value below
        final UID tag = uniqueIdCache.getUniqueIdOrDefault(oldNode.getTag());

        if (tag == null) {
            throw new RuntimeException(
                    "Cannot create a TagValueFilterTree with a tag that is not known in the UID cache. Tag: "
                            + oldNode.getTag());
        }

        final UID value = uniqueIdCache.getUniqueIdOrDefault(valueString);

        final RowKeyTagValue newNode = new RowKeyTagValue(tag, value);

        return newNode;
    }

    private static TagValueOperatorNode convertOperatorNode(final FilterTermsTree.OperatorNode oldNode,
            final UniqueIdCache uniqueIdCache) {

        final List<TagValueFilterTreeNode> children = new ArrayList<>();

        for (final FilterTermsTree.Node oldChild : oldNode.getChildren()) {
            children.add(convertNode(oldChild, uniqueIdCache));
        }
        final TagValueOperatorNode newNode = new TagValueOperatorNode(
                convertOperator(oldNode.getFilterOperationMode()),
                children);

        return newNode;
    }

    private static FilterOperationMode convertOperator(final FilterTermsTree.Operator operator) {
        return FilterOperationMode.valueOf(operator.name());
    }
}
