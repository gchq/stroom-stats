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

package stroom.stats.properties;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class StroomPropertyServiceImplIT extends AbstractHeadlessIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(StroomPropertyServiceImplIT.class);

    private StroomPropertyService stroomPropertyService;

    @Before
    public void setup() {
        stroomPropertyService = getApp().getInjector().getInstance(StroomPropertyService.class);
    }

    @Test
    public void getProperty() throws Exception {
        //given

       //when
       Optional<String> optStr = stroomPropertyService.getProperty("stroom.stats.hbase.config.zookeeper.quorum");

       //then
        assertThat(optStr).hasValue("localhost");
    }

    @Test
    public void setPropertyAndUpdate() throws Exception {
        //given
        String propName = "my.test.property";
        String propValue1 = "value1";
        String propValue2 = "value2";
        stroomPropertyService.setProperty(propName, propValue1);
        //there is a slight delay between setting a property and the tree cache getting updated
        sleep(Duration.ofMillis(50));

        //when
        String val = stroomPropertyService.getPropertyOrThrow(propName);

        //then
        assertThat(val).isEqualTo(propValue1);

        //when
        //update the property
        stroomPropertyService.setProperty(propName, propValue2);
        //there is a slight delay between setting a property and the tree cache getting updated
        sleep(Duration.ofMillis(50));
        val = stroomPropertyService.getPropertyOrThrow(propName);

        //then
        assertThat(val).isEqualTo(propValue2);
    }

    @Test
    public void getAllPropertyKeys() throws Exception {
        //when
        List<String> propKeys = stroomPropertyService.getAllPropertyKeys();

        //then
        assertThat(propKeys.size()).isGreaterThanOrEqualTo(1);
        propKeys.forEach(key -> {
            String val = stroomPropertyService.getPropertyOrThrow(key);
            assertThat(val).isNotEmpty();
        });

    }

}