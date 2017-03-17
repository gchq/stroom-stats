

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

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.FilterTermsTree.TermNode;
import stroom.stats.common.PrintableNode;
import stroom.stats.hbase.uid.MockUniqueIdCache;
import stroom.stats.hbase.uid.UID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public class TestTagValueFilterTreeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestTagValueFilterTreeBuilder.class);

    public static final String USER1 = "jack daniels";
    public static final String USER2 = "jim beam";
    public static final String FEED1 = "bourbon";
    public static final String FEED = "feed";
    public static final String USER = "user";

    // build a hashmap containing string to Byte[] conversions
    private static Map<String, UID> STRING_TO_UID_MAP = new HashMap<>();

    private final MockUniqueIdCache mockUniqueIdCache = new MockUniqueIdCache();

    static {
    }

    @Before
    public void setup() {
        createIdForName(USER);
        createIdForName(FEED);

        createIdForName(USER1);
        createIdForName(USER2);
        createIdForName(FEED1);
    }

    private void createIdForName(final String name) {
        final UID uid = mockUniqueIdCache.getOrCreateId(name);

        LOGGER.debug("Name: " + name + " UID: " + uid);

        STRING_TO_UID_MAP.put(name, uid);
    }

    @Test
    public void test_buildTagValueFilterTree() {
        // build a filter tree
        final PrintableNode termNodeUser1 = new TermNode(USER, USER1);
        final PrintableNode termNodeUser2 = new TermNode(USER, USER2);
        final PrintableNode termNodeFeed = new TermNode(FEED, FEED1);

        final List<PrintableNode> userNodes = new ArrayList<>();
        userNodes.add(termNodeUser1);
        userNodes.add(termNodeUser2);

        final OperatorNode usersNode = new OperatorNode(FilterOperationMode.OR, userNodes);

        final List<PrintableNode> childNodes = new ArrayList<>();
        childNodes.add(termNodeFeed);
        childNodes.add(usersNode);

        final OperatorNode rootNode = new OperatorNode(FilterOperationMode.AND, childNodes);

        final FilterTermsTree filterTermsTree = new FilterTermsTree(rootNode);

        // get a dump of what is in the tree
        final String oldTreeDump = filterTermsTree.toString();

        LOGGER.debug(oldTreeDump);

        // convert the filter tree into a tagValue version
        final TagValueFilterTree tagValueFilterTree = TagValueFilterTreeBuilder.buildTagValueFilterTree(filterTermsTree,
                mockUniqueIdCache);

        // dump this tree
        final String newTreeDump = tagValueFilterTree.toString();

        LOGGER.debug(newTreeDump);

        // manually hack the human readable dump of the old tree to make its
        // format look like
        // the new one and replace each string with a corresponding UID
        String oldTreeDumpReplaced = oldTreeDump.replace("=", " ");

        for (final Entry<String, UID> entry : STRING_TO_UID_MAP.entrySet()) {
            LOGGER.debug("Name: " + entry.getKey() + " UID: " + entry.getValue());

            oldTreeDumpReplaced = oldTreeDumpReplaced.replace(entry.getKey(), "[" + entry.getValue() + "]");
        }

        LOGGER.debug(oldTreeDumpReplaced);

        // compare the two strings to make sure they match
        assertEquals("ToString of new tree filter doesn't match the old one", oldTreeDumpReplaced, newTreeDump);

    }

    @Test
    public void test_buildTagValueFilterTree_nullFilterTermsTree() {
        final FilterTermsTree filterTermsTree = null;

        final TagValueFilterTree tagValueFilterTree = TagValueFilterTreeBuilder.buildTagValueFilterTree(filterTermsTree,
                mockUniqueIdCache);

        assertEquals(tagValueFilterTree, TagValueFilterTree.emptyTree());

    }

    @Test
    public void test_buildTagValueFilterTree_nullFilterTermsTreeRootNode() {
        final FilterTermsTree filterTermsTree = new FilterTermsTree(null);

        final TagValueFilterTree tagValueFilterTree = TagValueFilterTreeBuilder.buildTagValueFilterTree(filterTermsTree,
                mockUniqueIdCache);

        assertEquals(tagValueFilterTree, TagValueFilterTree.emptyTree());
    }
}
