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

package stroom.stats.streams;

import com.google.common.base.Preconditions;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.schema.Statistics;

import java.util.Optional;

/**
 * Simple wrapper class to hold both the statistic and its statisticConfiguration if it has one.
 * NOT designed to be serializable
 */
public class StatisticWrapper {

    private final Statistics.Statistic statistic;
    private final Optional<StatisticConfiguration> optStatConfig;
    private final long timeMs;
    private final Optional<String> validationErrorMessage;

    private StatisticWrapper(final Statistics.Statistic statistic, final Optional<StatisticConfiguration> optStatConfig,
                            final long timeMs, final Optional<String> validationErrorMessage) {
        Preconditions.checkNotNull(statistic);
        this.statistic = statistic;
        this.optStatConfig = optStatConfig;
        this.timeMs = timeMs;
        this.validationErrorMessage = validationErrorMessage;
    }

    public StatisticWrapper(final Statistics.Statistic statistic, final Optional<StatisticConfiguration> optStatConfig) {
        this(statistic, optStatConfig, statistic.getTime().toGregorianCalendar().getTimeInMillis(), Optional.empty());
    }

    public Statistics.Statistic getStatistic() {
        return statistic;
    }

    public Optional<StatisticConfiguration> getOptionalStatisticConfiguration() {
        return optStatConfig;
    }

    public long getTimeMs() {
        return timeMs;
    }

    /**
     * Return a new {@link StatisticWrapper} instance, with copied references, except for the addition of the passed
     * errorMessage
     */
    public StatisticWrapper addErrorMessage(String errorMessage){
        return new StatisticWrapper(statistic, optStatConfig, timeMs, Optional.of(errorMessage));
    }

    public boolean isValid() {
        return !validationErrorMessage.isPresent();
    }

    public Optional<String> getValidationErrorMessage() {
        return validationErrorMessage;
    }
}
