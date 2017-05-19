

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

import stroom.query.api.v1.ExpressionItem;
import stroom.query.api.v1.ExpressionOperator;
import stroom.query.api.v1.ExpressionTerm;
import stroom.query.api.v1.ExpressionTerm.Condition;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.FilterTermsTree.TermNode;

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
     * @param rootItem The {@link ExpressionItem} object that is the root of the tree
     * @return A {@link FilterTermsTree} object containing a tree of
     * {@link FilterTermsTree.Node} objects
     */
    public static FilterTermsTree convertExpresionItemsTree(final ExpressionOperator rootItem,
                                                            final Set<String> blackListedFieldNames) {
        final FilterTermsTree.Node newRootNode = convertNode(rootItem, blackListedFieldNames);

        // we may have black listed all our terms and been left with a null root
        // node so handle that
        return newRootNode != null ? new FilterTermsTree(newRootNode) : FilterTermsTree.emptyTree();
    }

    private static FilterTermsTree.Node convertNode(final ExpressionItem oldNode, final Set<String> fieldBlackList) {
        FilterTermsTree.Node newNode;

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

    private static FilterTermsTree.Node convertTermNode(final ExpressionTerm oldNode, final Set<String> fieldBlackList) {
        FilterTermsTree.Node newNode;

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
                        final List<FilterTermsTree.Node> orTermNodes = new ArrayList<>();

                        for (final String value : values) {
                            orTermNodes.add(convertTermNode(
                                    new ExpressionTerm(oldNode.getField(), Condition.EQUALS, value), fieldBlackList));
                        }
                        newNode = new OperatorNode(FilterTermsTree.Operator.OR, orTermNodes);
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
    private static FilterTermsTree.Node convertOperatorNode(final ExpressionOperator oldNode,
                                                            final Set<String> fieldBlackList) {
        // ExpressionOperator can be created with no child nodes so if that is
        // the case just return null and handle for
        // the null in the calling method

        if (oldNode.getChildren() == null || oldNode.getChildren().size() == 0) {
            return null;
        } else {
            final FilterTermsTree.Operator operator = convertOp(oldNode.getOp());

            final List<FilterTermsTree.Node> children = new ArrayList<>();

            for (final ExpressionItem oldChild : oldNode.getChildren()) {
                final FilterTermsTree.Node newChild = convertNode(oldChild, fieldBlackList);

                // if the newChild is null it means it was probably an
                // ExpressionOperator with no children
                if (newChild != null) {
                    children.add(newChild);
                }
            }

            FilterTermsTree.Node newNode = null;
            // term nodes may have been returned as null if they were expression
            // terms that this tree does not support
            if (children.size() == 1 && !operator.equals(FilterTermsTree.Operator.NOT)) {
                // only have one child for an AND or OR so no point in keeping
                // the operator node, just had the one child
                // to the tree instead
                newNode = children.get(0);
            } else if (!children.isEmpty()) {
                newNode = new OperatorNode(operator, children);
            }

            return newNode;
        }
    }

    private static FilterTermsTree.Operator convertOp(final ExpressionOperator.Op op) {
        return FilterTermsTree.Operator.valueOf(op.getDisplayValue());
    }
}
