

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

package stroom.stats.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to hold a tree of filter terms for use in stats retrieval. Due to the
 * way the stats are stored (by UID) it is only possible to do equals or not
 * equals on an object type/value pair
 */
public class FilterTermsTree {
    Node root;

    private static FilterTermsTree emptyTree;

    static {
        emptyTree = new FilterTermsTree(null);
    }

    public FilterTermsTree() {
    }

    public FilterTermsTree(final Node rootNode) {
        this.root = rootNode;
    }

    public static FilterTermsTree emptyTree() {
        return emptyTree;
    }

    public Node getRootNode() {
        return root;
    }

    public void setRootNode(final Node rootNode) {
        this.root = rootNode;
    }

    /**
     * Interface that is common to all nodes in the filter tree
     */
    public interface Node {
        @Override
        String toString();

        void printNode(StringBuilder sb);
    }

    /**
     * This class represents an actual filter term node in the filter tree, i.e.
     * X=Y
     */
    public static class TermNode implements Node {
        private final String tag;
        private final Condition condition;
        private final String value;

        //TODO will need to add a Condition enum into the TermNode to support wild carded and regex searches
        public TermNode(final String tag, final Condition condition, final String value) {
            if (tag == null) {
                throw new FilterTermsTreeException("Must have a tag to be added as a filter term");
            }

            this.tag = tag;
            this.condition = condition;
            this.value = value;
        }

        public TermNode(final String tag, final String value) {
            this(tag, Condition.EQUALS, value);
        }

        public String getTag() {
            return this.tag;
        }

        @SuppressWarnings("unused") //added in to save migration when regex matches are added
        public Condition getCondition() {
            return condition;
        }

        public String getValue() {
            return this.value;
        }

        @Override
        public void printNode(final StringBuilder sb) {
            final TermNode termNode = this;
            sb.append(termNode.getTag());
            sb.append("=");
            sb.append(termNode.getValue());
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            printNode(sb);
            return sb.toString();
        }
    }

    /**
     * This class represents an operator node in the filter tree, i.e.
     * AND/OR/NOT
     */
    public static class OperatorNode implements Node {
        private Operator filterOperationMode;
        private List<Node> children = new ArrayList<>();

        /**
         * Need to supply a list of children as there is no point in creating an
         * operator node without children
         *
         * @param childNodes
         */
        public OperatorNode(final Operator filterOperationMode, final List<Node> childNodes) {
            if (filterOperationMode.equals(Operator.NOT) && childNodes.size() < 1) {
                throw new FilterTermsTreeException("Cannot create an operator node with no child nodes");
            }
            // else if (filterOperationMode.equals(FilterOperationMode.OR) &&
            // childNodes.size() < 2) {
            // throw new FilterTermsTreeException(
            // "Cannot create an AND/OR operator node with less than two child
            // nodes");
            // }

            this.filterOperationMode = filterOperationMode;
            this.children.addAll(childNodes);
        }

        public OperatorNode(final Operator filterOperationMode, Node... childNodes) {
            this(filterOperationMode, Arrays.asList(childNodes));
        }

        public List<Node> getChildren() {
            return children;
        }

        public void setChildren(final List<Node> children) {
            this.children = children;
        }

        public Operator getFilterOperationMode() {
            return this.filterOperationMode;
        }

        public void setFilterOperationMode(final Operator filterOperationMode) {
            this.filterOperationMode = filterOperationMode;
        }

        @Override
        public void printNode(final StringBuilder sb) {
            final OperatorNode operatorNode = this;
            sb.append(" ");
            sb.append(operatorNode.getFilterOperationMode().toString());
            sb.append(" ");
            sb.append("(");

            // print each of the child nodes
            for (final Node childNode : operatorNode.getChildren()) {
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
            final StringBuilder sb = new StringBuilder();
            printNode(sb);
            return sb.toString();
        }
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

    public enum Operator {
        AND, OR, NOT
    }

    public enum Condition {
        EQUALS
       //TODO add REGEX_MATCH (non-match could be handled by NOT(REGEX_MATCH ...)
    }
}
