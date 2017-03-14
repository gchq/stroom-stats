

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

package stroom.stats.common;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * Not really testing anything, just confirming the behaviour of a Semaphore
 */
@Ignore
public class SemaphoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SemaphoreTest.class);

    @Test
    public void test() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE, true);

        final long start = System.currentTimeMillis();
        int j = 0;

        for (int i = 0; i < 10; i++) {
            // LOGGER.info("permits available : %s",
            // semaphore.availablePermits());

            semaphore.acquire(Integer.MAX_VALUE);

            // LOGGER.info("permits available : %s",
            // semaphore.availablePermits());

            semaphore.release(Integer.MAX_VALUE);

            // LOGGER.info("permits available : %s",
            // semaphore.availablePermits());
            j++;

            // LOGGER.info("------------------------------");
        }

        final long finish = System.currentTimeMillis();

        LOGGER.info("millis : {}, j: {}", (finish - start), j);

    }

    @Test
    public void test2() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE, true);

        semaphore.acquire(Integer.MAX_VALUE - 1);

        semaphore.acquire();

    }

    @Test
    public void test3() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE, true);

        semaphore.acquire();

        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final Runnable addThread = new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("addThread starting");
                    semaphore.acquire();
                    LOGGER.info("addThread got 1 permit");

                    Thread.sleep(2000);

                    semaphore.acquire(Integer.MAX_VALUE - 1);

                    LOGGER.info("addThread got all permits");

                    semaphore.release(Integer.MAX_VALUE);

                    LOGGER.info("addThread released all permits");
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

            }
        };

        final Runnable flushThread = new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("flushThread started");

                    semaphore.acquire(Integer.MAX_VALUE);

                    LOGGER.info("flushThread got all permits");

                    semaphore.release(Integer.MAX_VALUE);

                    LOGGER.info("flushThread released all permits");

                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }

            }
        };

        executor.execute(addThread);
        executor.execute(flushThread);
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.MINUTES);

        semaphore.acquire(Integer.MAX_VALUE - 1);
    }
}
