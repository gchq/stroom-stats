

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

package stroom.stats.configuration.marshaller;

import org.junit.Assert;
import org.junit.Test;
import stroom.stats.configuration.CustomRollUpMask;
import stroom.stats.configuration.CustomRollUpMaskEntityObject;
import stroom.stats.configuration.StatisticField;
import stroom.stats.configuration.StroomStatsStoreEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestStroomStatsStoreEntityMarshaller {
    @Test
    public void testUnmarshall() {
        StroomStatsStoreEntity stroomStatsStoreEntity = new StroomStatsStoreEntity();

        String str = "";
        str += "<?xml version=\"1.1\" encoding=\"UTF-8\"?>";
        str += "<data>";
        str += " <field>";
        str += "  <fieldName>user</fieldName>";
        str += " </field>";
        str += " <field>";
        str += "  <fieldName>feed</fieldName>";
        str += " </field>";
        str += " <customRollUpMask>";
        str += " </customRollUpMask>";
        str += " <customRollUpMask>";
        str += "  <rolledUpTagPosition>0</rolledUpTagPosition>";
        str += "  <rolledUpTagPosition>1</rolledUpTagPosition>";
        str += "  <rolledUpTagPosition>2</rolledUpTagPosition>";
        str += " </customRollUpMask>";
        str += " <customRollUpMask>";
        str += "  <rolledUpTagPosition>1</rolledUpTagPosition>";
        str += " </customRollUpMask>";
        str += " <customRollUpMask>";
        str += "  <rolledUpTagPosition>0</rolledUpTagPosition>";
        str += "  <rolledUpTagPosition>1</rolledUpTagPosition>";
        str += " </customRollUpMask>";
        str += "</data>";

        stroomStatsStoreEntity.setData(str);

        final StroomStatsStoreEntityMarshaller marshaller = new StroomStatsStoreEntityMarshaller();

        stroomStatsStoreEntity = marshaller.unmarshal(stroomStatsStoreEntity);

        Assert.assertNotNull(stroomStatsStoreEntity.getDataObject());

        final List<StatisticField> fields = stroomStatsStoreEntity.getDataObject()
                .getStatisticFields();
        final Set<? extends CustomRollUpMask> masks = stroomStatsStoreEntity.getDataObject()
                .getCustomRollUpMasks();

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals(4, masks.size());
        Assert.assertEquals("feed", fields.iterator().next().getFieldName());

        Assert.assertTrue(masks.contains(buildMask()));
        Assert.assertTrue(masks.contains(buildMask(0, 1, 2)));
        Assert.assertTrue(masks.contains(buildMask(1)));
        Assert.assertTrue(masks.contains(buildMask(0, 1)));

        // make sure the field positions are being cached when the object is
        // unmarshalled
        Assert.assertEquals(0, stroomStatsStoreEntity.getPositionInFieldList("feed").intValue());
        Assert.assertEquals(1, stroomStatsStoreEntity.getPositionInFieldList("user").intValue());

    }

    private CustomRollUpMask buildMask(final int... positions) {
        final List<Integer> list = new ArrayList<>();

        for (final int pos : positions) {
            list.add(pos);
        }

        return new CustomRollUpMaskEntityObject(list);
    }
}
