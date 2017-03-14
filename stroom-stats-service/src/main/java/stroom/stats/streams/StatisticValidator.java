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

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.schema.Statistics;
import stroom.stats.schema.TagType;

import java.util.stream.Collectors;

public class StatisticValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticValidator.class);

    /**
     * Make sure the event is correct for the statistic configuration
     */
//    public static boolean isValidEvent(final String statName, final StatisticWrapper statisticWrapper) {
    public static KeyValue<String, StatisticWrapper> validate(String statName, StatisticWrapper statisticWrapper) {


        if (statName == null) {
            return addErrorMsg(statName, statisticWrapper, "No statName in the message key");
        }
        Statistics.Statistic statistic = statisticWrapper.getStatistic();

        if (!statName.equals(statistic.getName())) {
            return addErrorMsg(statName, statisticWrapper, String.format("Stat name in the message key %s doesn't match name in the message %s", statName, statistic.getName()));
        }

        if (statisticWrapper.getOptionalStatisticConfiguration().isPresent()) {
            //we have a stat config
            StatisticConfiguration statisticConfiguration = statisticWrapper.getOptionalStatisticConfiguration().get();

            if (statisticConfiguration.getStatisticType().equals(StatisticType.COUNT) && statistic.getCount() == null) {
                return addErrorMsg(statName, statisticWrapper, String.format("Statistic is of type COUNT but getCount is null"));
            }
            if (statisticConfiguration.getStatisticType().equals(StatisticType.VALUE) && statistic.getValue() == null) {
                return addErrorMsg(statName, statisticWrapper, String.format("Statistic is of type VALUE but getValue is null"));
            }

            if (!statisticConfiguration.getFieldNames().containsAll(
                    statistic.getTags().getTag().stream()
                            .map(TagType::getName)
                            .collect(Collectors.toList()))) {
                return addErrorMsg(statName, statisticWrapper, String.format("The tag names in the event %s do not match those configured for the statistic %s",
                        statistic.getTags().getTag().stream()
                                .map(TagType::getName)
                                .collect(Collectors.joining(",")),
                        statisticConfiguration.getFieldNames().stream()
                                .collect(Collectors.joining(","))));
            }
        } else {
            return addErrorMsg(statName, statisticWrapper, String.format("No statistic configuration exists for name %s", statName));
        }
        LOGGER.trace("Message is valid");
        //just re-wrap the existing objects in a new KeyValue
        return new KeyValue<>(statName, statisticWrapper);
    }

    public static KeyValue<String, StatisticWrapper> addErrorMsg(String statName, StatisticWrapper statisticWrapper, String message) {
        return new KeyValue<>(statName, statisticWrapper.addErrorMessage(message));
    }

}
