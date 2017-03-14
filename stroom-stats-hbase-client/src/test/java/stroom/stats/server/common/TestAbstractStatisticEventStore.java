

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

package stroom.stats.server.common;

import org.junit.Assert;
import org.junit.Test;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.ExpressionTerm.Condition;
import stroom.query.api.Query;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.api.TimeAgnosticStatisticEvent;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.FindEventCriteria;
import stroom.stats.common.RolledUpStatisticEvent;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.MockCustomRollupMask;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.util.DateUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static stroom.query.api.ExpressionOperator.*;

public class TestAbstractStatisticEventStore {
    private static final long EVENT_TIME = 1234L;
    private static final String EVENT_NAME = "MyStatistic";
    private static final long EVENT_COUNT = 1;

    private static final String TAG1_NAME = "Tag1";
    private static final String TAG1_VALUE = "Tag1Val";

    private static final String TAG2_NAME = "Tag2";
    private static final String TAG2_VALUE = "Tag2Val";

    private static final String ROLLED_UP_VALUE = RollUpBitMask.ROLL_UP_TAG_VALUE;

    @Test
    public void testGenerateTagRollUpsTwoTagsAllRollUps() {
        final StatisticEvent event = buildEvent(buildTagList());

        final StatisticConfiguration statisticConfiguration = buildStatisticConfiguration();

        final RolledUpStatisticEvent rolledUpStatisticEvent = AbstractStatisticsService.generateTagRollUps(event,
                statisticConfiguration);

        assertEquals(4, rolledUpStatisticEvent.getPermutationCount());

        final List<TimeAgnosticStatisticEvent> timeAgnosticStatisticEvents = new ArrayList<>();

        // define all the tag/value perms we expect to get back
        final List<List<StatisticTag>> expectedTagPerms = new ArrayList<>();
        expectedTagPerms
                .add(Arrays.asList(new StatisticTag(TAG1_NAME, TAG1_VALUE), new StatisticTag(TAG2_NAME, TAG2_VALUE)));
        expectedTagPerms.add(
                Arrays.asList(new StatisticTag(TAG1_NAME, TAG1_VALUE), new StatisticTag(TAG2_NAME, ROLLED_UP_VALUE)));
        expectedTagPerms.add(
                Arrays.asList(new StatisticTag(TAG1_NAME, ROLLED_UP_VALUE), new StatisticTag(TAG2_NAME, TAG2_VALUE)));
        expectedTagPerms.add(Arrays.asList(new StatisticTag(TAG1_NAME, ROLLED_UP_VALUE),
                new StatisticTag(TAG2_NAME, ROLLED_UP_VALUE)));

        System.out.println("-------------------------------------------------");
        for (final List<StatisticTag> expectedTagList : expectedTagPerms) {
            System.out.println(expectedTagList);
        }
        System.out.println("-------------------------------------------------");

        for (final TimeAgnosticStatisticEvent eventPerm : rolledUpStatisticEvent) {
            // make sure we don't already have one like this.
            assertFalse(timeAgnosticStatisticEvents.contains(eventPerm));

            assertEquals(event.getName(), eventPerm.getName());
            assertEquals(event.getType(), eventPerm.getType());
            assertEquals(event.getCount(), eventPerm.getCount());
            assertEquals(event.getTagList().size(), eventPerm.getTagList().size());

            System.out.println(eventPerm.getTagList());

            assertTrue(expectedTagPerms.contains(eventPerm.getTagList()));

            for (int i = 0; i < event.getTagList().size(); i++) {
                assertEquals(event.getTagList().get(i).getTag(), eventPerm.getTagList().get(i).getTag());
                Assert.assertTrue(event.getTagList().get(i).getValue().equals(eventPerm.getTagList().get(i).getValue())
                        || RollUpBitMask.ROLL_UP_TAG_VALUE.equals(eventPerm.getTagList().get(i).getValue()));
            }
        }
    }

    @Test
    public void testGenerateTagRollUpsTwoTagsCustomRollUps() {
        final StatisticEvent event = buildEvent(buildTagList());

        final StatisticConfiguration statisticConfiguration = buildStatisticConfiguration(StatisticRollUpType.CUSTOM);

        final RolledUpStatisticEvent rolledUpStatisticEvent = AbstractStatisticsService.generateTagRollUps(event,
                statisticConfiguration);

        assertEquals(3, rolledUpStatisticEvent.getPermutationCount());

        final List<TimeAgnosticStatisticEvent> timeAgnosticStatisticEvents = new ArrayList<>();

        // define all the tag/value perms we expect to get back
        final List<List<StatisticTag>> expectedTagPerms = new ArrayList<>();

        // nothing rolled up
        expectedTagPerms
                .add(Arrays.asList(new StatisticTag(TAG1_NAME, TAG1_VALUE), new StatisticTag(TAG2_NAME, TAG2_VALUE)));

        // tag 2 rolled up
        expectedTagPerms.add(
                Arrays.asList(new StatisticTag(TAG1_NAME, TAG1_VALUE), new StatisticTag(TAG2_NAME, ROLLED_UP_VALUE)));

        // tags 1 and 2 rolled up
        expectedTagPerms.add(Arrays.asList(new StatisticTag(TAG1_NAME, ROLLED_UP_VALUE),
                new StatisticTag(TAG2_NAME, ROLLED_UP_VALUE)));

        System.out.println("-------------------------------------------------");
        for (final List<StatisticTag> expectedTagList : expectedTagPerms) {
            System.out.println(expectedTagList);
        }
        System.out.println("-------------------------------------------------");

        for (final TimeAgnosticStatisticEvent eventPerm : rolledUpStatisticEvent) {
            // make sure we don't already have one like this.
            assertFalse(timeAgnosticStatisticEvents.contains(eventPerm));

            assertEquals(event.getName(), eventPerm.getName());
            assertEquals(event.getType(), eventPerm.getType());
            assertEquals(event.getCount(), eventPerm.getCount());
            assertEquals(event.getTagList().size(), eventPerm.getTagList().size());

            System.out.println(eventPerm.getTagList());

            assertTrue(expectedTagPerms.contains(eventPerm.getTagList()));

            for (int i = 0; i < event.getTagList().size(); i++) {
                assertEquals(event.getTagList().get(i).getTag(), eventPerm.getTagList().get(i).getTag());
                Assert.assertTrue(event.getTagList().get(i).getValue().equals(eventPerm.getTagList().get(i).getValue())
                        || RollUpBitMask.ROLL_UP_TAG_VALUE.equals(eventPerm.getTagList().get(i).getValue()));
            }
        }
    }

    @Test
    public void testGenerateTagRollUpsNoTags() {
        final StatisticEvent event = buildEvent(new ArrayList<StatisticTag>());

        final StatisticConfiguration statisticConfiguration = buildStatisticConfiguration();

        final RolledUpStatisticEvent rolledUpStatisticEvent = AbstractStatisticsService.generateTagRollUps(event,
                statisticConfiguration);

        assertEquals(1, rolledUpStatisticEvent.getPermutationCount());

        for (final TimeAgnosticStatisticEvent eventPerm : rolledUpStatisticEvent) {
            assertEquals(event.getName(), eventPerm.getName());
            assertEquals(event.getType(), eventPerm.getType());
            assertEquals(event.getCount(), eventPerm.getCount());
            assertEquals(event.getTagList().size(), eventPerm.getTagList().size());

            for (int i = 0; i < event.getTagList().size(); i++) {
                assertEquals(event.getTagList().get(i).getTag(), eventPerm.getTagList().get(i).getTag());
                Assert.assertTrue(event.getTagList().get(i).getValue().equals(eventPerm.getTagList().get(i).getValue())
                        || RollUpBitMask.ROLL_UP_TAG_VALUE.equals(eventPerm.getTagList().get(i).getValue()));
            }
        }

    }

    @Test
    public void testGenerateTagRollUpsNotEnabled() {
        final StatisticEvent event = buildEvent(buildTagList());

        final StatisticConfiguration statisticConfiguration = buildStatisticConfiguration(StatisticRollUpType.NONE);

        final RolledUpStatisticEvent rolledUpStatisticEvent = AbstractStatisticsService.generateTagRollUps(event,
                statisticConfiguration);

        assertEquals(1, rolledUpStatisticEvent.getPermutationCount());

        for (final TimeAgnosticStatisticEvent eventPerm : rolledUpStatisticEvent) {
            assertEquals(event.getName(), eventPerm.getName());
            assertEquals(event.getType(), eventPerm.getType());
            assertEquals(event.getCount(), eventPerm.getCount());
            assertEquals(event.getTagList().size(), eventPerm.getTagList().size());

            for (int i = 0; i < event.getTagList().size(); i++) {
                assertEquals(event.getTagList().get(i).getTag(), eventPerm.getTagList().get(i).getTag());
                Assert.assertTrue(event.getTagList().get(i).getValue().equals(eventPerm.getTagList().get(i).getValue())
                        || RollUpBitMask.ROLL_UP_TAG_VALUE.equals(eventPerm.getTagList().get(i).getValue()));
            }
        }
    }


    private StatisticEvent buildEvent(final List<StatisticTag> tagList) {
        return new StatisticEvent(EVENT_TIME, EVENT_NAME, tagList, EVENT_COUNT);
    }

    private List<StatisticTag> buildTagList() {
        final List<StatisticTag> tagList = new ArrayList<StatisticTag>();

        tagList.add(new StatisticTag(TAG1_NAME, TAG1_VALUE));
        tagList.add(new StatisticTag(TAG2_NAME, TAG2_VALUE));

        return tagList;
    }

    private StatisticConfiguration buildStatisticConfiguration() {
        return buildStatisticConfiguration(StatisticRollUpType.ALL);
    }

    private StatisticConfiguration buildStatisticConfiguration(final StatisticRollUpType statisticRollUpType) {
        final MockStatisticConfiguration statisticConfiguration = new MockStatisticConfiguration();


        statisticConfiguration.addFieldName(TAG1_NAME);
        statisticConfiguration.addFieldName(TAG2_NAME);


        // add the custom rollup masks, which only come into play if the type is
        // CUSTOM
        statisticConfiguration.addCustomRollupMask(new MockCustomRollupMask(new ArrayList<Integer>())); // no
        statisticConfiguration.addCustomRollupMask(new MockCustomRollupMask(Arrays.asList(0, 1))); // tags
        statisticConfiguration.addCustomRollupMask(new MockCustomRollupMask(Arrays.asList(1))); // tag
                                                                                                // 2
        statisticConfiguration.setRollUpType(statisticRollUpType);

        return statisticConfiguration;
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBuildCriteria_noDate() throws Exception {
        final ExpressionOperator rootOperator = new ExpressionOperator(true, Op.AND, Arrays.asList());

        final Query query = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        AbstractStatisticsService.buildCriteria(query, dataSource);

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

        AbstractStatisticsService.buildCriteria(query, dataSource);

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

        final FindEventCriteria criteria = AbstractStatisticsService.buildCriteria(query, dataSource);

        Assert.assertNotNull(criteria);
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

        final Query search = new Query(null, rootOperator, null);

        final MockStatisticConfiguration dataSource = new MockStatisticConfiguration();
        dataSource.setName("MyDataSource");

        AbstractStatisticsService.buildCriteria(search, dataSource);
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

        AbstractStatisticsService.buildCriteria(query, dataSource);
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

        final FindEventCriteria criteria = AbstractStatisticsService.buildCriteria(query, dataSource);

        Assert.assertNotNull(criteria);
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

        final FindEventCriteria criteria = AbstractStatisticsService.buildCriteria(query, dataSource);

        Assert.assertNotNull(criteria);
        Assert.assertEquals(fromDate, criteria.getPeriod().getFrom().longValue());
        Assert.assertEquals(toDate + 1, criteria.getPeriod().getTo().longValue());

        // only a date term so the filter tree has noting in it as the date is
        // handled outside of the tree
        Assert.assertEquals("[MyField=xxx]", criteria.getFilterTermsTree().toString());
    }
}
