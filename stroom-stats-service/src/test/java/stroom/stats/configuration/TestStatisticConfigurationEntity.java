

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

package stroom.stats.configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class TestStatisticConfigurationEntity {
    private static final String FIELD1 = "field1";
    private static final String FIELD2 = "field2";
    private static final String FIELD3 = "field3";

    @Test
    public void testIsValidFieldPass() {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        final String fieldToTest = FIELD1;

        Assert.assertTrue(sds.isValidField(fieldToTest));
    }

    @Test
    public void testIsValidFieldFailBadFieldName() {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        final String fieldToTest = "BadFieldName";

        Assert.assertFalse(sds.isValidField(fieldToTest));
    }

    @Test
    public void testIsValidFieldFailNoFields() {
        // build with no fields
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(false);

        final String fieldToTest = "BadFieldName";

        Assert.assertFalse(sds.isValidField(fieldToTest));
    }

    @Test
    public void testListOrder1() {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        Assert.assertEquals(
                Arrays.asList(new StatisticField(FIELD1), new StatisticField(FIELD2), new StatisticField(FIELD3)),
                new ArrayList<>(sds.getStatisticDataSourceDataObject().getStatisticFields()));

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD2, FIELD3), getFieldNames(sds));

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD2, FIELD3), sds.getFieldNames());
    }

    @Test
    public void testListOrder2() {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        Assert.assertEquals(
                Arrays.asList(new StatisticField(FIELD1), new StatisticField(FIELD2), new StatisticField(FIELD3)),
                sds.getStatisticDataSourceDataObject().getStatisticFields());

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD2, FIELD3), getFieldNames(sds));

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD2, FIELD3), sds.getFieldNames());

        // remove an item and check the order

        sds.getStatisticDataSourceDataObject().removeStatisticField(new StatisticField(FIELD2));

        Assert.assertEquals(Arrays.asList(new StatisticField(FIELD1), new StatisticField(FIELD3)),
                sds.getStatisticDataSourceDataObject().getStatisticFields());

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD3), getFieldNames(sds));

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD3), sds.getFieldNames());

        // add an item back in and check the order

        sds.getStatisticDataSourceDataObject().addStatisticField(new StatisticField(FIELD2));

        Assert.assertEquals(
                Arrays.asList(new StatisticField(FIELD1), new StatisticField(FIELD2), new StatisticField(FIELD3)),
                sds.getStatisticDataSourceDataObject().getStatisticFields());

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD2, FIELD3), getFieldNames(sds));

        Assert.assertEquals(Arrays.asList(FIELD1, FIELD2, FIELD3), sds.getFieldNames());
    }

    private List<String> getFieldNames(final StatisticConfigurationEntity sds) {
        final List<String> list = new ArrayList<>();
        for (final StatisticField statisticField : sds.getStatisticDataSourceDataObject().getStatisticFields()) {
            list.add(statisticField.getFieldName());
        }
        return list;
    }

    @Test
    public void testFieldPositions() {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        Assert.assertEquals(0, sds.getPositionInFieldList(FIELD1).intValue());
        Assert.assertEquals(1, sds.getPositionInFieldList(FIELD2).intValue());
        Assert.assertEquals(2, sds.getPositionInFieldList(FIELD3).intValue());

        sds.getStatisticDataSourceDataObject().removeStatisticField(new StatisticField(FIELD2));

        Assert.assertEquals(0, sds.getPositionInFieldList(FIELD1).intValue());
        Assert.assertEquals(null, sds.getPositionInFieldList(FIELD2));
        Assert.assertEquals(1, sds.getPositionInFieldList(FIELD3).intValue());

        sds.getStatisticDataSourceDataObject().addStatisticField(new StatisticField(FIELD2));

        Assert.assertEquals(0, sds.getPositionInFieldList(FIELD1).intValue());
        Assert.assertEquals(1, sds.getPositionInFieldList(FIELD2).intValue());
        Assert.assertEquals(2, sds.getPositionInFieldList(FIELD3).intValue());
    }

    @Test
    public void testIsRollUpCombinationSupported_nullList() throws Exception {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        Assert.assertTrue(sds.isRollUpCombinationSupported(null));
    }

    @Test
    public void testIsRollUpCombinationSupported_emptyList() throws Exception {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        Assert.assertTrue(sds.isRollUpCombinationSupported(new HashSet<>()));
    }

    @Test
    public void testIsRollUpCombinationSupported_rollUpTypeAll() throws Exception {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        sds.setRollUpType(StatisticRollUpType.ALL);

        Assert.assertTrue(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD1))));
    }

    @Test
    public void testIsRollUpCombinationSupported_rollUpTypeNone() throws Exception {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        sds.setRollUpType(StatisticRollUpType.NONE);

        Assert.assertFalse(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD1))));
    }

    @Test
    public void testIsRollUpCombinationSupported_rollUpTypeCustom() throws Exception {
        final StatisticConfigurationEntity sds = buildStatisticConfiguration(true);

        sds.setRollUpType(StatisticRollUpType.CUSTOM);

        // check it copes in or out of order
        Assert.assertTrue(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD1, FIELD2, FIELD3))));
        Assert.assertTrue(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD2, FIELD3, FIELD1))));

        // check the other valid combinations
        Assert.assertTrue(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD1, FIELD2))));
        Assert.assertTrue(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD1))));

        Assert.assertFalse(sds.isRollUpCombinationSupported(new HashSet<>(Arrays.asList(FIELD3))));
    }

    private StatisticConfigurationEntity buildStatisticConfiguration(final boolean addFields) {
        final StatisticConfigurationEntityData statisticConfigurationEntityData = new StatisticConfigurationEntityData();

        if (addFields) {
            statisticConfigurationEntityData.addStatisticField(new StatisticField(FIELD2));
            statisticConfigurationEntityData.addStatisticField(new StatisticField(FIELD3));
            statisticConfigurationEntityData.addStatisticField(new StatisticField(FIELD1));

            statisticConfigurationEntityData.addCustomRollUpMask(new CustomRollUpMaskEntityObject(Arrays.asList(0, 1, 2))); // fields
                                                                                                        // 1,2,3
            statisticConfigurationEntityData.addCustomRollUpMask(new CustomRollUpMaskEntityObject(Arrays.asList(0, 1))); // fields
                                                                                                        // 1,2
            statisticConfigurationEntityData.addCustomRollUpMask(new CustomRollUpMaskEntityObject(Arrays.asList(0))); // field
                                                                                                    // 1
            statisticConfigurationEntityData.addCustomRollUpMask(new CustomRollUpMaskEntityObject(Collections.<Integer> emptyList()));
        }

        final StatisticConfigurationEntity sds = new StatisticConfigurationEntity();
        sds.setStatisticDataSourceDataObject(statisticConfigurationEntityData);
        return sds;
    }
}
