

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


import stroom.stats.api.StatisticType;

public interface InMemoryEventStoreIdPool {
    /**
     * Returns an ID from the pool. It will return the ID at the top of the
     * internal stack, so typically the ID last returned to the pool
     *
     * @return An ID from 0-n where n=poolSize-1
     * @throws InterruptedException
     */
    int getId(StatisticType statisticType) throws InterruptedException;

    /**
     * Returns an ID to the pool for use by other threads. This method should be
     * used within a finally block to ensure the ID is returned in the event of
     * a failure.
     *
     * @param id
     *            The ID to return
     */
    void returnId(StatisticType statisticType, int id);

    /**
     * If not already disabled, disables the pool such that all subsequent getId
     * calls will block until enablePool is called.
     */
    void disablePool(StatisticType statisticType);

    /**
     * Will enable the pool, if disabled, releasing any blocked getId calls
     */
    void enablePool(StatisticType statisticType);

    int getPoolSize(StatisticType statisticType);

    /**
     * @return Returns true if the pool is enabled. If enabled IDs will be
     *         released when available. If disabled calls to getId will block,
     *         but calls to returnId will not.
     */
    boolean getEnabledState(StatisticType statisticType);

    /**
     * @return The number of permits currently available for use. Only to be
     *         used for monitoring or debugging.
     */
    int getAvailablePermitCount(StatisticType statisticType);
}
