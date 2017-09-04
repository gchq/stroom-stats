package stroom.stats;

import org.junit.Assert;
import org.junit.Test;
import stroom.query.api.v2.ExpressionOperator;
import stroom.query.api.v2.ExpressionOperator.Op;
import stroom.query.api.v2.ExpressionTerm;
import stroom.query.api.v2.ExpressionTerm.Condition;
import stroom.query.api.v2.Query;
import stroom.query.api.v2.SearchRequest;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.util.DateUtil;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

public class TestHBaseClient {

    @Test
    public void testBuildCriteria_noDate() throws Exception {
        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND, Arrays.asList());

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        SearchStatisticsCriteria criteria = HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);

        assertNotNull(criteria);
        assertThat(criteria.getPeriod().getFrom()).isNull();
        assertThat(criteria.getPeriod().getTo()).isNull();

        // only a date term so the filter tree has noting in it as the date is
        // handled outside of the tree
        Assert.assertEquals(FilterTermsTree.emptyTree(), criteria.getFilterTermsTree());

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildCriteria_invalidDateCondition() throws Exception {
        final String dateTerm = "2000-01-01T00:00:00.000Z,2010-01-01T00:00:00.000Z";
        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME,
                        Condition.IN_DICTIONARY, dateTerm)
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);

    }

    @Test
    public void testBuildCriteria_validDateTerm() throws Exception {
        final String fromDateStr = "2000-01-01T00:00:00.000Z";
        final long fromDate = DateUtil.parseNormalDateTimeString(fromDateStr);
        final String toDateStr = "2010-01-01T00:00:00.000Z";
        final long toDate = DateUtil.parseNormalDateTimeString(toDateStr);

        final String dateTerm = fromDateStr + "," + toDateStr;

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm)
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        final SearchStatisticsCriteria criteria = HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);

        assertNotNull(criteria);
        Assert.assertEquals(fromDate, criteria.getPeriod().getFrom().longValue());
        Assert.assertEquals(toDate + 1, criteria.getPeriod().getTo().longValue());

        // only a date term so the filter tree has noting in it as the date is
        // handled outside of the tree
        Assert.assertEquals(FilterTermsTree.emptyTree(), criteria.getFilterTermsTree());
    }

    @Test(expected = RuntimeException.class)
    public void testBuildCriteria_invalidDateTermOnlyOneDate() throws Exception {

        final String fromDateStr = "2000-01-01T00:00:00.000Z";

        final String dateTerm = fromDateStr;

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm)
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildCriteria_validDateTermOtherTermMissingFieldName() throws Exception {

        final String fromDateStr = "2000-01-01T00:00:00.000Z";
        final long fromDate = DateUtil.parseNormalDateTimeString(fromDateStr);
        final String toDateStr = "2010-01-01T00:00:00.000Z";
        final long toDate = DateUtil.parseNormalDateTimeString(toDateStr);

        final String dateTerm = fromDateStr + "," + toDateStr;

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm),
                new ExpressionTerm(null, Condition.EQUALS, "xxx")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);
    }

    @Test
    public void testBuildCriteria_validDateTermOtherTermMissingFieldValue() throws Exception {
        final String fromDateStr = "2000-01-01T00:00:00.000Z";
        final long fromDate = DateUtil.parseNormalDateTimeString(fromDateStr);
        final String toDateStr = "2010-01-01T00:00:00.000Z";
        final long toDate = DateUtil.parseNormalDateTimeString(toDateStr);

        final String dateTerm = fromDateStr + "," + toDateStr;

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm),
                new ExpressionTerm("MyField", Condition.EQUALS, "")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        final SearchStatisticsCriteria criteria = HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);

        assertNotNull(criteria);
        Assert.assertEquals("[MyField=]", criteria.getFilterTermsTree().toString());
    }

    @Test
    public void testBuildCriteria_validDateTermAndOtherTerm() throws Exception {
        final String fromDateStr = "2000-01-01T00:00:00.000Z";
        final long fromDate = DateUtil.parseNormalDateTimeString(fromDateStr);
        final String toDateStr = "2010-01-01T00:00:00.000Z";
        final long toDate = DateUtil.parseNormalDateTimeString(toDateStr);

        final String dateTerm = fromDateStr + "," + toDateStr;

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm),
                new ExpressionTerm("MyField", Condition.EQUALS, "xxx")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        final SearchStatisticsCriteria criteria = HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildCriteria_dateTermTooDeep() throws Exception {
        final String fromDateStr = "2000-01-01T00:00:00.000Z";
        final long fromDate = DateUtil.parseNormalDateTimeString(fromDateStr);
        final String toDateStr = "2010-01-01T00:00:00.000Z";
        final long toDate = DateUtil.parseNormalDateTimeString(toDateStr);

        final String dateTerm = fromDateStr + "," + toDateStr;

        final ExpressionOperator childOp = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm),
                new ExpressionTerm("MyField1", Condition.EQUALS, "xxx")
        );

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                childOp,
                new ExpressionTerm("MyField2", Condition.EQUALS, "xxx")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildCriteria_tooManyDateTerms() throws Exception {
        final String fromDateStr = "2000-01-01T00:00:00.000Z";
        final long fromDate = DateUtil.parseNormalDateTimeString(fromDateStr);
        final String toDateStr = "2010-01-01T00:00:00.000Z";
        final long toDate = DateUtil.parseNormalDateTimeString(toDateStr);

        final String dateTerm = fromDateStr + "," + toDateStr;

        final ExpressionOperator childOp = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm),
                new ExpressionTerm("MyField1", Condition.EQUALS, "xxx")
        );

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm),
                new ExpressionTerm("MyField2", Condition.EQUALS, "xxx"),
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_DATE_TIME, Condition.BETWEEN, dateTerm)
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildCriteria_precisionTooDeep() throws Exception {

        final ExpressionOperator childOp = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION, Condition.EQUALS, "minute"),
                new ExpressionTerm("MyField1", Condition.EQUALS, "xxx")
        );

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                childOp,
                new ExpressionTerm("MyField2", Condition.EQUALS, "xxx")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildCriteria_tooManyPrecisionTerms() throws Exception {

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION, Condition.EQUALS, "minute"),
                new ExpressionTerm("MyField2", Condition.EQUALS, "xxx"),
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION, Condition.EQUALS, "minute")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildCriteria_wrongPrecisionCondition() throws Exception {

        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND,
                new ExpressionTerm(StatisticConfiguration.FIELD_NAME_PRECISION, Condition.IN_DICTIONARY, "minute"),
                new ExpressionTerm("MyField2", Condition.EQUALS, "xxx")
        );

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        HBaseClient.buildCriteria(wrapQuery(query), Collections.emptyList(), dataSource);
    }

    private SearchRequest wrapQuery(Query query) {
        return new SearchRequest(null, query, Collections.emptyList(), ZoneOffset.UTC.getId(), false);
    }
}