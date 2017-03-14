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

package stroom.stats.cache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.junit.Test;
import stroom.stats.properties.MockStroomPropertyService;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheConfigurationService {


    private static final String CACHE_NAME = "myCache";
    private MockStroomPropertyService mockStroomPropertyService = new MockStroomPropertyService();

    private CacheConfigurationService cacheConfigurationService = new CacheConfigurationServiceImpl(mockStroomPropertyService);

    @Test
    public void newCacheConfigurationBuilder_oneExpiryThreeResourcePools() throws Exception {

        //given
        addProp(CacheConfigurationServiceImpl.PROP_KEY_SUFFIX_TIME_TO_IDLE_SECS,60);

        addProp(CacheConfigurationServiceImpl.PROP_KEY_SUFFIX_MAX_ENTRIES_HEAP,1);
        addProp(CacheConfigurationServiceImpl.PROP_KEY_SUFFIX_MAX_MB_OFF_HEAP,2);
        addProp(CacheConfigurationServiceImpl.PROP_KEY_SUFFIX_MAX_MB_DISK,3);

        //when

        CacheConfigurationBuilder<String,String> builder = cacheConfigurationService.newCacheConfigurationBuilder(CACHE_NAME, String.class, String.class);
        CacheConfiguration<String, String> cacheConfiguration = builder.build();

        //then
        assertThat(cacheConfiguration.getResourcePools().getResourceTypeSet().size()).isEqualTo(3);
        assertThat(cacheConfiguration.getExpiry()).isEqualTo(Expirations.timeToIdleExpiration(Duration.of(60, TimeUnit.SECONDS)));

    }

    @Test(expected = RuntimeException.class)
    public void newCacheConfigurationBuilder_twoExpiries() throws Exception {

        //given
        addProp(CacheConfigurationServiceImpl.PROP_KEY_SUFFIX_TIME_TO_IDLE_SECS,60);
        addProp(CacheConfigurationServiceImpl.PROP_KEY_SUFFIX_TIME_TO_LIVE_SECS,120);

        //when

        CacheConfigurationBuilder<String,String> builder = cacheConfigurationService.newCacheConfigurationBuilder(CACHE_NAME, String.class, String.class);
        CacheConfiguration<String, String> cacheConfiguration = builder.build();

        //then

        //nothing as an exception is throw

    }

    private void addProp(String suffix, int value) {
        mockStroomPropertyService.setProperty(CacheConfigurationServiceImpl.buildKey(CACHE_NAME, suffix), Integer.toString(value));
    }

}