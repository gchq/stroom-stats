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
import stroom.stats.api.StatisticType;
import stroom.stats.hbase.aggregator.EventStoresThreadFlushAggregator;

import javax.inject.Inject;

@SuppressWarnings("unused")
@On("0 0/10 0 * * ?")
class IdPoolStateCheckJob extends Job {

    private final EventStoresThreadFlushAggregator eventStoresThreadFlushAggregator;

    @SuppressWarnings("unused")
    @Inject
    IdPoolStateCheckJob(final EventStoresThreadFlushAggregator eventStoresThreadFlushAggregator) {
        this.eventStoresThreadFlushAggregator = eventStoresThreadFlushAggregator;
    }

    @Override
    public void doJob() {
        for (StatisticType statisticType : StatisticType.values()) {
            eventStoresThreadFlushAggregator.enableDisableIdPool(statisticType);
        }
    }
}
