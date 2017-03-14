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

package stroom.stats.jobs;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.On;
import stroom.stats.api.StatisticsService;
import stroom.stats.cluster.ClusterLockService;
import stroom.stats.configuration.StatisticConfigurationService;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.util.logging.LogExecutionTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

@SuppressWarnings("unused")
//Fire at 07:05 each day
@On("0 5 7 * * ?")
class RetentionPurgeJob extends Job{

    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionPurgeJob.class);

    private final StatisticsService statisticsService;
    private final StatisticConfigurationService statisticConfigurationService;
    private final ClusterLockService clusterLockService;

    /**
     * The cluster lock to acquire to prevent other nodes from concurrently
     * aggregating statistics.
     */
    private static final String LOCK_NAME = RetentionPurgeJob.class.getName();

    @SuppressWarnings("unused")
    @Inject
    RetentionPurgeJob(final StatisticsService statisticsService,
                      final StatisticConfigurationService statisticConfigurationService, final ClusterLockService clusterLockService) {
        this.statisticsService = statisticsService;
        this.statisticConfigurationService = statisticConfigurationService;
        this.clusterLockService = clusterLockService;
    }

    @Override
    public void doJob() {
        final LogExecutionTime logExecutionTime = new LogExecutionTime();

        LOGGER.info("Event Store Purge - start");
        if (clusterLockService.tryLock(LOCK_NAME)) {
            try {
                List<StatisticConfiguration> statisticConfigurations = statisticConfigurationService.fetchAll();
                statisticsService.purgeOldData(statisticConfigurations);
                LOGGER.info("Event Store Purge - finished in {}", logExecutionTime);
            } catch (final Throwable t) {
                LOGGER.error(t.getMessage(), t);
            } finally {
                clusterLockService.releaseLock(LOCK_NAME);
            }
        } else {
            LOGGER.info("Event Store Purge - Skipped as did not get lock in {}", logExecutionTime);
        }
    }
}
