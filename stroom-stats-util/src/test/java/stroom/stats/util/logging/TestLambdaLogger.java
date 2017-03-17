

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

package stroom.stats.util.logging;

import org.junit.Assert;
import org.junit.Test;
import stroom.stats.util.test.TestUtils;

import java.util.concurrent.atomic.AtomicInteger;

public class TestLambdaLogger {


    @Test
    public void testInterval() {
        final LambdaLogger stroomLogger = LambdaLogger.getLogger(TestLambdaLogger.class);
        stroomLogger.setInterval(1);
        Assert.assertTrue(stroomLogger.checkInterval());
        Assert.assertTrue(TestUtils.sleep(100));
        Assert.assertTrue(stroomLogger.checkInterval());
    }

    private String produceMessage(final String level) {
        return "this is my big " + level + " msg";
    }

    @Test
    public void testMessageSupplier() {
        final LambdaLogger stroomLogger = LambdaLogger.getLogger(TestLambdaLogger.class);

        stroomLogger.trace(() -> produceMessage("trace"));
        stroomLogger.trace(() -> produceMessage("trace"), new Throwable());
        stroomLogger.debug(() -> produceMessage("debug"));
        stroomLogger.debug(() -> produceMessage("debug"), new Throwable());
        stroomLogger.warn(() -> produceMessage("warn"));
        stroomLogger.warn(() -> produceMessage("warn"), new Throwable());
        stroomLogger.error(() -> produceMessage("error"));
        stroomLogger.error(() -> produceMessage("error"), new Throwable());

    }

    @Test
    public void testIfTraceIsEnabled() {
        final LambdaLogger stroomLogger = LambdaLogger.getLogger(TestLambdaLogger.class);

        final AtomicInteger counter = new AtomicInteger(0);

        stroomLogger.ifTraceIsEnabled(() -> counter.incrementAndGet());

        Assert.assertEquals(stroomLogger.isTraceEnabled() ? 1 : 0, counter.get());
    }

    @Test
    public void testIfDebugIsEnabled() {
        final LambdaLogger stroomLogger = LambdaLogger.getLogger(TestLambdaLogger.class);

        final AtomicInteger counter = new AtomicInteger(0);

        stroomLogger.ifDebugIsEnabled(() -> counter.incrementAndGet());

        Assert.assertEquals(stroomLogger.isDebugEnabled() ? 1 : 0, counter.get());
    }

}
