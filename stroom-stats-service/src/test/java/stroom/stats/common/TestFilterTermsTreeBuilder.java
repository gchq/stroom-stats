

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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.query.api.v2.ExpressionItem;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionOperator.Op;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.ExpressionTerm.Condition;
import stroom.stats.common.FilterTermsTree.Node;
import stroom.stats.common.FilterTermsTree.OperatorNode;
import stroom.stats.common.FilterTermsTree.TermNode;
import stroom.stats.configuration.StatisticConfiguration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFilterTermsTreeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestFilterTermsTreeBuilder.class);

    Set<String> fieldBlackList = new HashSet<>(Arrays.asList(StatisticConfiguration.FIELD_NAME_DATE_TIME));

    /**
     * Verify that a tree of {@link ExpressionItem} objects can be converted
     * correctly into a {@link FilterTermsTree}
     */
    @Test
    public void testConvertExpresionItemsTree() {
        // AND (op1)
        // --term1field IN term1value1,term1value2,term1value3
        // --OR (op2)
        // ----term2field=term2value
        // ----term3field=term3value
        // ----NOT (op3)
        // ---- term4field=term4value

        // should convert to

        // AND (op1)
        // --OR
        // ----term1field=term1value1
        // ----term1field=term1value2
        // ----term1field=term1value3
        // --OR (op2)
        // ----term2field=term2value
        // ----term3field=term3value
        // ----NOT (op3)
        // ---- term4field=term4value

        final ExpressionTerm term4 = new ExpressionTerm("term4field", Condition.EQUALS, "term4value");
        final ExpressionTerm term3 = new ExpressionTerm("term3field", Condition.EQUALS, "term3value");
        final ExpressionTerm term2 = new ExpressionTerm("term2field", Condition.EQUALS, "term2value");

        final String term1value1 = "term1value1";
        final String term1value2 = "term1value2";
        final String term1value3 = "term1value3";

        final ExpressionTerm term1 = new ExpressionTerm("term1field",
                Condition.IN, term1value1 + "," + term1value2 + "," + term1value3);

        final ExpressionOperator op3 = new ExpressionOperator(true, Op.NOT, term4);

        final ExpressionOperator op2 = new ExpressionOperator(true, Op.OR, term2, term3, op3);

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, op2);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1);

        final OperatorNode newOp1 = (OperatorNode) filterTermsTree.getRootNode();

        assertEquals(op1.getOp().getDisplayValue(), newOp1.getFilterOperationMode().toString());

        final OperatorNode newTerm1OpNode = (OperatorNode) newOp1.getChildren().get(0);
        assertEquals(Op.OR.toString(), newTerm1OpNode.getFilterOperationMode().toString());
        assertEquals(3, newTerm1OpNode.getChildren().size());

        final TermNode newTerm1SubTerm1 = (TermNode) newTerm1OpNode.getChildren().get(0);
        final TermNode newTerm1SubTerm2 = (TermNode) newTerm1OpNode.getChildren().get(1);
        final TermNode newTerm1SubTerm3 = (TermNode) newTerm1OpNode.getChildren().get(2);

        assertEquals(term1.getField(), newTerm1SubTerm1.getTag());
        assertEquals(term1value1, newTerm1SubTerm1.getValue());
        assertEquals(term1.getField(), newTerm1SubTerm2.getTag());
        assertEquals(term1value2, newTerm1SubTerm2.getValue());
        assertEquals(term1.getField(), newTerm1SubTerm3.getTag());
        assertEquals(term1value3, newTerm1SubTerm3.getValue());

        final OperatorNode newOp2 = (OperatorNode) newOp1.getChildren().get(1);

        assertEquals(op2.getOp().getDisplayValue(), newOp2.getFilterOperationMode().toString());

        final TermNode newTerm2 = (TermNode) newOp2.getChildren().get(0);
        final TermNode newTerm3 = (TermNode) newOp2.getChildren().get(1);
        final OperatorNode newOp3 = (OperatorNode) newOp2.getChildren().get(2);

        assertEquals(term2.getField(), newTerm2.getTag());
        assertEquals(term2.getValue(), newTerm2.getValue());
        assertEquals(term3.getField(), newTerm3.getTag());
        assertEquals(term3.getValue(), newTerm3.getValue());
        assertEquals(op3.getOp().getDisplayValue(), newOp3.getFilterOperationMode().toString());

        final TermNode newTerm4 = (TermNode) newOp3.getChildren().get(0);
        assertEquals(term4.getField(), newTerm4.getTag());
        assertEquals(term4.getValue(), newTerm4.getValue());
    }

    @Test
    public void testEmptyExpressionTree() {
        // AND (op1)

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, Arrays.asList());

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1);

    }

    /**
     * Should fail as a non-datetime field is using a condition other than
     * equals
     */
    @Test(expected = RuntimeException.class)
    public void testInvalidCondition() {
        // AND (op1)
        // --term1 - datetime equals 123456789
        // --term2 - field1 between 1 and 2

        final ExpressionTerm term1 = new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Condition.EQUALS, "123456789");
        final ExpressionTerm term2 = new ExpressionTerm("term2field", Condition.BETWEEN, "1,2");

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, term2);

        FilterTermsTreeBuilder.convertExpresionItemsTree(op1, fieldBlackList);
    }

    @Test
    public void testNonEqualsConditionForDatetimeField() {
        // AND (op1)
        // --term1 - datetime between 1 and 2
        // --term2 - field1 equals 123456789

        final ExpressionTerm term1 = new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Condition.BETWEEN, "1,2");
        final ExpressionTerm term2 = new ExpressionTerm("term2field", Condition.EQUALS, "123456789");

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, term2);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1, fieldBlackList);

        // if we get here without an exception then it has worked as planned
        assertTrue(filterTermsTree != null);

    }

    @Test
    public void testInConditionOneValue() {
        final ExpressionTerm term1 = new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, "1,2");
        final ExpressionTerm term2 = new ExpressionTerm("term1field", Condition.IN, "123456789");

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, term2);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1, fieldBlackList);

        final TermNode term2Node = (TermNode) filterTermsTree.getRootNode();

        assertEquals("term1field", term2Node.getTag());
        assertEquals("123456789", term2Node.getValue());

    }

    @Test
    public void testInConditionNoValue() {
        final ExpressionTerm term1 = new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Condition.BETWEEN, "1,2");
        final ExpressionTerm term2 = new ExpressionTerm("term1field", Condition.IN, "");

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, term2);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1, fieldBlackList);

        final TermNode term2Node = (TermNode) filterTermsTree.getRootNode();

        assertEquals("term1field", term2Node.getTag());
        assertEquals("", term2Node.getValue());
    }

    @Test
    public void testDisabledBranch() {
        final ExpressionTerm term1 = new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Condition.BETWEEN, "1,2");
        final ExpressionTerm term2 = new ExpressionTerm("term2field", Condition.IN, "2");
        final ExpressionTerm term3 = new ExpressionTerm("term3field", Condition.IN, "3");

        final ExpressionTerm term4 = new ExpressionTerm("term4field", Condition.IN, "4");
        final ExpressionTerm term5 = new ExpressionTerm("term5field", Condition.IN, "5");
        final ExpressionOperator disabledBranch = new ExpressionOperator(false, Op.AND, term4, term5);

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, term2, term3, disabledBranch);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1, fieldBlackList);

        LOGGER.debug(filterTermsTree.toString());

        //date term is blacklisted, disabled branch is ignored so only terms 2 and 3 feature
        List<Node> level1Nodes = checkNode(filterTermsTree.getRootNode(), FilterTermsTree.Operator.AND, 2);
        checkNode(level1Nodes.get(0), term2.getField(), FilterTermsTree.Condition.EQUALS, term2.getValue());
        checkNode(level1Nodes.get(1), term3.getField(), FilterTermsTree.Condition.EQUALS, term3.getValue());
    }

    @Test
    public void testDisabledTerms() {
        final ExpressionTerm term1 = new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME,
                Condition.BETWEEN, "1,2");
        final ExpressionTerm term2 = new ExpressionTerm(true, "term2field", Condition.IN, "2", null);
        final ExpressionTerm term3 = new ExpressionTerm(false, "term3field", Condition.IN, "3", null);
        final ExpressionTerm term4 = new ExpressionTerm(true, "term4field", Condition.IN, "4", null);
        final ExpressionTerm term5 = new ExpressionTerm(false, "term5field", Condition.IN, "5", null);

        final ExpressionOperator op1 = new ExpressionOperator(true, Op.AND, term1, term2, term3, term4, term5);

        final FilterTermsTree filterTermsTree = FilterTermsTreeBuilder.convertExpresionItemsTree(op1, fieldBlackList);

        LOGGER.debug(filterTermsTree.toString());

        //date term is blacklisted, terms 3 and 5 are disabled, so only 2 and 4 remain.
        List<Node> level1Nodes = checkNode(filterTermsTree.getRootNode(), FilterTermsTree.Operator.AND, 2);
        checkNode(level1Nodes.get(0), term2.getField(), FilterTermsTree.Condition.EQUALS, term2.getValue());
        checkNode(level1Nodes.get(1), term4.getField(), FilterTermsTree.Condition.EQUALS, term4.getValue());
    }

    private List<Node> checkNode(Node node, FilterTermsTree.Operator expectedOperator, int expectedChildCount) {
        assertThat(node).isInstanceOf(OperatorNode.class);
        OperatorNode operatorNode = (OperatorNode) node;
        assertThat(operatorNode.getFilterOperationMode()).isEqualTo(expectedOperator);
        assertThat(operatorNode.getChildren()).hasSize(expectedChildCount);
        return operatorNode.getChildren();
    }

    private TermNode checkNode(Node node,
                           String expectedTag,
                           FilterTermsTree.Condition expectedCondition,
                           String expectedValue) {
        assertThat(node).isInstanceOf(TermNode.class);
        TermNode termNode = (TermNode) node;
        assertThat(termNode.getTag()).isEqualTo(expectedTag);
        assertThat(termNode.getCondition()).isEqualTo(expectedCondition);
        assertThat(termNode.getValue()).isEqualTo(expectedValue);
        return termNode;
    }
}
