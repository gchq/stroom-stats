

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

package stroom.stats.api;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestTimeAgnosticStatisticEvent {
    @Test
    public void serialiseTest() {
        final List<StatisticTag> tagList = new ArrayList<>();
        tagList.add(new StatisticTag("tag1", "val1"));
        tagList.add(new StatisticTag("tag2", "val2"));

        final TimeAgnosticStatisticEvent timeAgnosticStatisticEvent = new TimeAgnosticStatisticEvent("MyStatName",
                tagList, 42L);

        // if we can't serialise the object then we should get an exception here
        final byte[] serializedForm = SerializationUtils.serialize(timeAgnosticStatisticEvent);

        final TimeAgnosticStatisticEvent timeAgnosticStatisticEvent2 = (TimeAgnosticStatisticEvent) SerializationUtils
                .deserialize(serializedForm);

        Assert.assertEquals(timeAgnosticStatisticEvent, timeAgnosticStatisticEvent2);
    }
}
