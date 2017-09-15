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

package stroom.stats.streams;

import com.google.common.base.Preconditions;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.schema.v3.Statistics;

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
        //hold the converted time to save doing the conversion again
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
