

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

package stroom.stats.hbase.aggregator;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

public class TestIdPool {
    private static final Random RANDOM = new Random();

    private static final Logger LOGGER = LoggerFactory.getLogger(TestIdPool.class);

    @Test
    public void test() {
        // pool of 40 threads
        final ExecutorService executorService = Executors.newFixedThreadPool(100);
        final int poolSize = 10;

        final IdPool idPool = new IdPool(poolSize);

        final AtomicInteger concurrentThreads = new AtomicInteger(0);

        final AtomicBoolean hasFailed = new AtomicBoolean(false);

        for (int i = 0; i < 100; i++) {
            final Runnable task = new Runnable() {
                @Override
                public void run() {
                    // LOGGER.debug("Running task: " + taskId);

                    int idFromPool = -1;

                    try {
                        try {
                            idFromPool = idPool.getId();

                            // keep a running count of the number of concurrent
                            // threads and note if we exceed the number
                            // in the pool
                            concurrentThreads.incrementAndGet();

                            if (concurrentThreads.get() > poolSize) {
                                hasFailed.set(true);
                            }

                            // LOGGER.debug("----Using pool ID: " + idFromPool);

                            // sleep for a small random length of time,
                            Thread.sleep(RANDOM.nextInt(100));

                        } catch (final InterruptedException e) {
                            LOGGER.error("Interupted", e);
                            Thread.currentThread().interrupt();
                        }

                        concurrentThreads.decrementAndGet();
                        idPool.returnId(idFromPool);
                    } catch (final Exception e) {
                        hasFailed.set(true);
                        LOGGER.error("Got an exception in the thread", e);
                    }
                }
            };

            // LOGGER.debug("Queued task " + i);
            executorService.execute(task);

        }

        executorService.shutdown();

        if (hasFailed.get()) {
            fail();
        }

        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Assert.fail("Should have completed by now");
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testEnableDisable() throws InterruptedException {
        // pool of 40 threads
        final ExecutorService executorService = Executors.newFixedThreadPool(100);
        final int poolSize = 10;

        final IdPool idPool = new IdPool(poolSize);

        final AtomicInteger concurrentThreads = new AtomicInteger(0);
        final AtomicInteger getCount = new AtomicInteger(0);

        final AtomicBoolean hasFailed = new AtomicBoolean(false);

        final Runnable getIdTask = new Runnable() {
            @Override
            public void run() {
                // LOGGER.debug("Running task: " + taskId);

                int idFromPool = -1;

                try {
                    try {
                        idFromPool = idPool.getId();

                        // keep a running count of the number of concurrent
                        // threads and note if we exceed the number
                        // in the pool
                        concurrentThreads.incrementAndGet();
                        getCount.incrementAndGet();

                        // sleep for a small random length of time,
                        Thread.sleep(RANDOM.nextInt(100));

                    } catch (final InterruptedException e) {
                        LOGGER.error("Interupted", e);
                        Thread.currentThread().interrupt();
                    }

                    concurrentThreads.decrementAndGet();
                    idPool.returnId(idFromPool);
                } catch (final Exception e) {
                    LOGGER.error("Got an exception in the thread", e);
                }
            }
        };

        final Future<?> future = executorService.submit(getIdTask);

        while (!future.isDone()) {
            // wait for task to finish
        }

        Assert.assertEquals(0, concurrentThreads.get());
        Assert.assertEquals(1, getCount.get());

        idPool.disablePool();

        Assert.assertFalse(idPool.getEnabledState());

        // fire off two 'readers' which should block as the pool is disabled
        final Future<?> blockedFuture1 = executorService.submit(getIdTask);
        final Future<?> blockedFuture2 = executorService.submit(getIdTask);

        Thread.sleep(200);

        Assert.assertFalse(blockedFuture1.isDone());
        Assert.assertFalse(blockedFuture2.isDone());

        Assert.assertEquals(0, concurrentThreads.get());
        Assert.assertEquals(1, getCount.get());

        // this should release the readers
        idPool.enablePool();

        Assert.assertTrue(idPool.getEnabledState());

        executorService.execute(getIdTask);

        executorService.shutdown();

        if (hasFailed.get()) {
            fail();
        }

        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);

        } catch (final InterruptedException e) {
            Assert.fail("Should have completed by now");
            Thread.currentThread().interrupt();
        }

        Assert.assertEquals(0, concurrentThreads.get());
        Assert.assertEquals(4, getCount.get());
    }

    @Test(expected = RuntimeException.class)
    public void testReturnId() throws InterruptedException {
        final IdPool idPool = new IdPool(10);

        int id = idPool.getId();

        id++;

        // should throw a runtime exception as the ID is not the one it took
        idPool.returnId(id);
    }

    @Test
    public void testMultipleDisableCalls() {
        final IdPool idPool = new IdPool(10);

        idPool.disablePool();

        idPool.disablePool();

    }

    @Test
    public void testMultipleEnableCalls() {
        final IdPool idPool = new IdPool(10);

        idPool.enablePool();

        idPool.enablePool();

    }

    @Test
    public void testPermitCount() throws InterruptedException {
        final IdPool idPool = new IdPool(3);

        idPool.disablePool();

        idPool.enablePool();

        final int id1 = idPool.getId();
        final int id2 = idPool.getId();
        final int id3 = idPool.getId();

        final AtomicInteger getCount = new AtomicInteger(0);

        final Runnable getIdTask = new Runnable() {
            @Override
            public void run() {
                // LOGGER.debug("Running task: " + taskId);

                int idFromPool = -1;

                try {
                    try {
                        idFromPool = idPool.getId();

                        getCount.incrementAndGet();

                    } catch (final InterruptedException e) {
                        LOGGER.error("Interupted", e);
                        Thread.currentThread().interrupt();
                    }
                    idPool.returnId(idFromPool);
                } catch (final Exception e) {
                    LOGGER.error("Got an exception in the thread", e);
                }
            }
        };
        final ExecutorService executorService = Executors.newFixedThreadPool(100);
        final Future<?> blockedFuture1 = executorService.submit(getIdTask);
        final Future<?> blockedFuture2 = executorService.submit(getIdTask);
        final Future<?> blockedFuture3 = executorService.submit(getIdTask);

        Thread.sleep(200);

        // should all be waiting as all three IDs from the pool are out already.
        Assert.assertFalse(blockedFuture1.isDone());
        Assert.assertFalse(blockedFuture2.isDone());
        Assert.assertFalse(blockedFuture3.isDone());
        Assert.assertEquals(0, getCount.get());

        // this should free up the three other threads
        idPool.returnId(id1);
        idPool.returnId(id2);
        idPool.returnId(id3);

        executorService.shutdown();

        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);

        } catch (final InterruptedException e) {
            Assert.fail("Should have completed by now");
            Thread.currentThread().interrupt();
        }

        Assert.assertTrue(blockedFuture1.isDone());
        Assert.assertTrue(blockedFuture2.isDone());
        Assert.assertTrue(blockedFuture3.isDone());
        Assert.assertEquals(3, getCount.get());
    }

    /**
     * Noddy test to confirm the behaviour of a semaphore
     */
    @Test
    public void testSemaphoreAddTooMany() {
        final Semaphore semaphore = new Semaphore(1);
        System.out.println(semaphore.availablePermits());

        semaphore.release(5);
        System.out.println(semaphore.availablePermits());
    }

    /**
     * Noddy test to confirm the behaviour of a semaphore
     */
    @Test
    public void testSemaphoreDrainAndRefill() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(10);

        semaphore.acquire();

        Assert.assertEquals(9, semaphore.availablePermits());

        semaphore.drainPermits();

        Assert.assertEquals(0, semaphore.availablePermits());

        semaphore.release(10);
        Assert.assertEquals(10, semaphore.availablePermits());

        semaphore.acquire();

        Assert.assertEquals(9, semaphore.availablePermits());
    }

    /**
     * Checking that we always get the same ID for repeated executions if there
     * is only one thread active
     */
    @Test
    public void testSingleThreadSameId() throws InterruptedException {
        final int poolSize = 10;

        final IdPool idPool = new IdPool(poolSize);

        for (int i = 1; i <= 100; i++) {
            final int id = idPool.getId();
            Assert.assertEquals(poolSize - 1, id);
            Assert.assertEquals(poolSize - 1, idPool.getAvailablePermitCount());
            idPool.returnId(id);
            Assert.assertEquals(poolSize, idPool.getAvailablePermitCount());
        }
    }

    @Test
    public void testGetPoolSize() throws Exception {
        final int poolSize = 10;

        final IdPool idPool = new IdPool(poolSize);

        Assert.assertEquals(poolSize, idPool.getPoolSize());
    }
}
