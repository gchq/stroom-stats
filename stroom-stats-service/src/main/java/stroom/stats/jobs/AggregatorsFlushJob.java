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

package stroom.stats.jobs;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.On;
import stroom.stats.hbase.aggregator.EventStoresPutAggregator;
import stroom.stats.hbase.aggregator.EventStoresThreadFlushAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

@SuppressWarnings("unused")
//Every minute
@On("0 * * * * ?")
class AggregatorsFlushJob extends Job{

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregatorsFlushJob.class);

    private final EventStoresPutAggregator eventStoresPutAggregator;
    private final EventStoresThreadFlushAggregator eventStoresThreadFlushAggregator;

    @SuppressWarnings("unused")
    @Inject
    AggregatorsFlushJob(final EventStoresPutAggregator eventStoresPutAggregator,
                               final EventStoresThreadFlushAggregator eventStoresThreadFlushAggregator) {
        this.eventStoresPutAggregator = eventStoresPutAggregator;
        this.eventStoresThreadFlushAggregator = eventStoresThreadFlushAggregator;
    }

    @Override
    public void doJob() {
        LOGGER.debug("Aggregator flush cron job firing");

        if (eventStoresPutAggregator != null) {
            eventStoresPutAggregator.flushQueue();
        }

        if (eventStoresThreadFlushAggregator != null) {
            eventStoresThreadFlushAggregator.flushQueue();
        }
    }
}
