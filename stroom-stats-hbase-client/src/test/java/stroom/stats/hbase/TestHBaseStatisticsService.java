

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

package stroom.stats.hbase;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import stroom.query.api.ExpressionOperator;
import stroom.query.api.ExpressionTerm;
import stroom.query.api.ExpressionTerm.Condition;
import stroom.query.api.Query;
import stroom.query.api.SearchRequest;
import stroom.stats.api.StatisticEvent;
import stroom.stats.api.StatisticTag;
import stroom.stats.common.FilterTermsTree;
import stroom.stats.common.SearchStatisticsCriteria;
import stroom.stats.common.rollup.RollUpBitMask;
import stroom.stats.configuration.MockCustomRollupMask;
import stroom.stats.configuration.MockStatisticConfiguration;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.util.DateUtil;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static stroom.query.api.ExpressionOperator.Op;

public class TestHBaseStatisticsService {
    private static final long EVENT_TIME = 1234L;
    private static final String EVENT_NAME = "MyStatistic";
    private static final long EVENT_COUNT = 1;

    private static final String TAG1_NAME = "Tag1";
    private static final String TAG1_VALUE = "Tag1Val";

    private static final String TAG2_NAME = "Tag2";
    private static final String TAG2_VALUE = "Tag2Val";

    private static final String ROLLED_UP_VALUE = RollUpBitMask.ROLL_UP_TAG_VALUE;



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

}
