

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

package stroom.stats.hbase.table.filter;

import stroom.stats.hbase.structure.TagValueFilterTreeNode;


/**
 * Class to hold a tree of filter terms for use in stats retrieval. Due to the
 * way the stats are stored (by UID) it is only possible to do equals or not
 * equals on an object type/value pair
 */
public class TagValueFilterTree {
    private TagValueFilterTreeNode root;

    private static TagValueFilterTree emptyTree;

    static {
        emptyTree = new TagValueFilterTree(null);
    }

    public TagValueFilterTree(final TagValueFilterTreeNode rootNode) {
        this.root = rootNode;
    }

    public static TagValueFilterTree emptyTree() {
        return emptyTree;
    }

    public TagValueFilterTreeNode getRootNode() {
        return root;
    }

    public static class FilterTermsTreeException extends RuntimeException {
        private static final long serialVersionUID = 8955006804383215661L;

        public FilterTermsTreeException(final String message) {
            super(message);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (root != null) {
            root.printNode(sb);
        }
        sb.append("]");

        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TagValueFilterTree that = (TagValueFilterTree) o;

        return root != null ? root.equals(that.root) : that.root == null;
    }

    @Override
    public int hashCode() {
        return root != null ? root.hashCode() : 0;
    }
}
