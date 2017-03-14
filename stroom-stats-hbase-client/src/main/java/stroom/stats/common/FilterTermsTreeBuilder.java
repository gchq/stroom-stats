

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

package stroom.stats.common;

import stroom.query.api.ExpressionItem;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.ExpressionTerm.Condition;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.FilterTermsTree.TermNode;
import stroom.stats.hbase.table.filter.FilterOperationMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FilterTermsTreeBuilder {
    private FilterTermsTreeBuilder() {
        // Utility class of static methods so should never be initialised
    }

    static FilterTermsTree convertExpresionItemsTree(final ExpressionOperator rootItem) {
        return convertExpresionItemsTree(rootItem, Collections.emptySet());
    }

    /**
     * Converts a tree of {@link ExpressionItem} objects into a
     * {@link FilterTermsTree}. Conversion is not exact as some features of the
     * {@link ExpressionItem} tree are not supported so it may throw a
     * {@link RuntimeException}.
     *
     * @param rootItem
     *            The {@link ExpressionItem} object that is the root of the tree
     * @return A {@link FilterTermsTree} object containing a tree of
     *         {@link PrintableNode} objects
     */
    public static FilterTermsTree convertExpresionItemsTree(final ExpressionOperator rootItem,
            final Set<String> blackListedFieldNames) {
        final PrintableNode newRootNode = convertNode(rootItem, blackListedFieldNames);

        // we may have black listed all our terms and been left with a null root
        // node so handle that
        return newRootNode != null ? new FilterTermsTree(newRootNode) : FilterTermsTree.emptyTree();
    }

    private static PrintableNode convertNode(final ExpressionItem oldNode, final Set<String> fieldBlackList) {
        PrintableNode newNode;

        if (oldNode.enabled()) {
            if (oldNode instanceof ExpressionTerm) {
                final ExpressionTerm termNode = (ExpressionTerm) oldNode;
                newNode = convertTermNode(termNode, fieldBlackList);
            } else if (oldNode instanceof ExpressionOperator) {
                newNode = convertOperatorNode((ExpressionOperator) oldNode, fieldBlackList);
            } else {
                throw new RuntimeException("Node is of a type that we don't expect: " + oldNode.getClass().getName());
            }
        } else {
            // the node is disabled so just return null rather than including it
            // in the new tree
            newNode = null;
        }

        return newNode;
    }

    private static PrintableNode convertTermNode(final ExpressionTerm oldNode, final Set<String> fieldBlackList) {
        PrintableNode newNode;

        if (fieldBlackList != null && fieldBlackList.contains(oldNode.getField())) {
            // this term is black listed so ignore it
            newNode = null;
        } else {
            // TODO we could convert a CONTAINS ExpressionTerm to multiple
            // TermNodes contained within an OR
            // OperatorNode. e.g. if the ExpressionTerm is 'CONTAINS "apache"'
            // then we go to the UID cache to find
            // all
            // tag values that contain apache and included them all as OR
            // TermNodes. We cannot propagate partial
            // matches
            // any further down the stack as we can only filter on distinct UIDs

            if (oldNode.getCondition().equals(Condition.EQUALS)) {
                newNode = new TermNode(oldNode.getField(), oldNode.getValue());
            } else if (oldNode.getCondition().equals(Condition.IN)) {
                if (oldNode.getValue() == null) {
                    newNode = new TermNode(oldNode.getField(), null);
                } else {
                    final String[] values = oldNode.getValue().split(Condition.IN_CONDITION_DELIMITER);

                    if (values.length == 1) {
                        // only one value so just convert it like it is EQUALS
                        newNode = new TermNode(oldNode.getField(), oldNode.getValue());
                    } else {
                        // multiple values in the IN list so convert it into a
                        // set of EQUALS terms under and OR node
                        final List<PrintableNode> orTermNodes = new ArrayList<>();

                        for (final String value : values) {
                            orTermNodes.add(convertTermNode(
                                    new ExpressionTerm(oldNode.getField(), Condition.EQUALS, value), fieldBlackList));
                        }
                        newNode = new OperatorNode(FilterOperationMode.OR, orTermNodes);
                    }
                }
            } else {
                throw new UnsupportedOperationException("Only EQUALS and IN are currently supported");
            }
        }
        return newNode;
    }

    /**
     * @return The converted node, null if the old node has no children
     */
    private static PrintableNode convertOperatorNode(final ExpressionOperator oldNode,
            final Set<String> fieldBlackList) {
        // ExpressionOperator can be created with no child nodes so if that is
        // the case just return null and handle for
        // the null in the calling method

        if (oldNode.getChildren() == null || oldNode.getChildren().size() == 0) {
            return null;
        } else {
            final FilterOperationMode operationMode = FilterOperationMode.valueOf(oldNode.getOp().getDisplayValue().toString());

            final List<PrintableNode> children = new ArrayList<>();

            for (final ExpressionItem oldChild : oldNode.getChildren()) {
                final PrintableNode newChild = convertNode(oldChild, fieldBlackList);

                // if the newChild is null it means it was probably an
                // ExpressionOperator with no children
                if (newChild != null) {
                    children.add(newChild);
                }
            }

            PrintableNode newNode = null;
            // term nodes may have been returned as null if they were expression
            // terms that this tree does not support
            if (children.size() == 1 && !operationMode.equals(FilterOperationMode.NOT)) {
                // only have one child for an AND or OR so no point in keeping
                // the operator node, just had the one child
                // to the tree instead
                newNode = children.get(0);
            } else if (!children.isEmpty()) {
                newNode = new OperatorNode(operationMode, children);
            }

            return newNode;
        }
    }
}
