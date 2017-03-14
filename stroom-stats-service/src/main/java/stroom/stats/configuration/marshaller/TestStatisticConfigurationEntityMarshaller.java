

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

package stroom.stats.configuration.marshaller;

import org.junit.Assert;
import org.junit.Test;
import stroom.stats.configuration.CustomRollUpMask;
import stroom.stats.configuration.CustomRollUpMaskEntityObject;
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticField;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestStatisticConfigurationEntityMarshaller {
    @Test
    public void testUnmarshall() {
        StatisticConfigurationEntity statisticConfigurationEntity = new StatisticConfigurationEntity();

        String str = "";
        str += "<?xml version=\"1.1\" encoding=\"UTF-8\"?>";
        str += "<data>";
        str += " <field>";
        str += "  <fieldName>feed</fieldName>";
        str += " </field>";
        str += " <field>";
        str += "  <fieldName>user</fieldName>";
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

        statisticConfigurationEntity.setData(str);

        final StatisticConfigurationEntityMarshaller marshaller = new StatisticConfigurationEntityMarshaller();

        statisticConfigurationEntity = marshaller.unmarshal(statisticConfigurationEntity);

        Assert.assertNotNull(statisticConfigurationEntity.getStatisticDataSourceDataObject());

        final List<StatisticField> fields = statisticConfigurationEntity.getStatisticDataSourceDataObject()
                .getStatisticFields();
        final Set<? extends CustomRollUpMask> masks = statisticConfigurationEntity.getStatisticDataSourceDataObject()
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
        Assert.assertEquals(0, statisticConfigurationEntity.getPositionInFieldList("feed").intValue());
        Assert.assertEquals(1, statisticConfigurationEntity.getPositionInFieldList("user").intValue());

    }

    private CustomRollUpMask buildMask(final int... positions) {
        final List<Integer> list = new ArrayList<>();

        for (final int pos : positions) {
            list.add(pos);
        }

        return new CustomRollUpMaskEntityObject(list);
    }
}
