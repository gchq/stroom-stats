

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

import stroom.stats.hbase.structure.TagValueFilterTreeNode;
import stroom.stats.hbase.table.filter.TagValueFilterTree.FilterTermsTreeException;

import java.util.ArrayList;
import java.util.List;

public class TagValueOperatorNode implements TagValueFilterTreeNode {
    private FilterOperationMode filterOperationMode;

    private List<TagValueFilterTreeNode> children = new ArrayList<>();


    /**
     * Need to supply a list of children as there is no point in creating an
     * operator node without children
     *
     * @param childNodes
     */
    public TagValueOperatorNode(FilterOperationMode filterOperationMode, List<TagValueFilterTreeNode> childNodes) {
        if (filterOperationMode.equals(FilterOperationMode.NOT) && childNodes.size() != 1) {
            throw new FilterTermsTreeException(
                    "A NOT operator node must have exactly 1 child nodes, this one has " + childNodes.size());
        } else if (!filterOperationMode.equals(FilterOperationMode.NOT) && childNodes.size() < 2) {
            throw new FilterTermsTreeException("Cannot create an AND/OR operator node with less than two child nodes");
        }

        this.filterOperationMode = filterOperationMode;
        this.children.addAll(childNodes);
    }

    public List<TagValueFilterTreeNode> getChildren() {
        return children;
    }

    public FilterOperationMode getFilterOperationMode() {
        return this.filterOperationMode;
    }

    @Override
    public void printNode(StringBuilder sb) {
        TagValueOperatorNode operatorNode = this;
        sb.append(" ");
        sb.append(operatorNode.getFilterOperationMode().toString());
        sb.append(" ");
        sb.append("(");

        // print each of the child nodes
        for (TagValueFilterTreeNode childNode : operatorNode.getChildren()) {
            childNode.printNode(sb);
            sb.append(",");
        }
        // remove the trailing comma
        if (sb.charAt(sb.length() - 1) == ',')
            sb.deleteCharAt(sb.length() - 1);

        sb.append(")");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        printNode(sb);
        sb.append(")");

        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TagValueOperatorNode that = (TagValueOperatorNode) o;

        if (filterOperationMode != that.filterOperationMode) return false;
        return children.equals(that.children);
    }

    @Override
    public int hashCode() {
        int result = filterOperationMode.hashCode();
        result = 31 * result + children.hashCode();
        return result;
    }
}
