

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

package stroom.stats.server.common;


import stroom.stats.api.StatisticType;
import stroom.stats.common.StatisticConfigurationValidator;
import stroom.stats.configuration.StatisticConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticConfigurationValidatorImpl implements StatisticConfigurationValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticConfigurationValidatorImpl.class);

    @Override
    public boolean validateStatisticConfiguration(final String statisticName, final String engineName,
                                                  final StatisticType statisticType, final StatisticConfiguration statisticConfiguration) {
        if (statisticConfiguration == null) {
            LOGGER.warn("No StatisticDataSource could be found with name {} and engine {}, so no statistics will be recorded for it.",
                    statisticName, engineName);
            return false;
        } else if (!statisticConfiguration.getStatisticType().equals(statisticType)) {
            LOGGER.error("The type of the event [{}] is not valid for the StatisticDataSource with name {}, engine {} and type {}.",
                    statisticType, statisticName, engineName, statisticConfiguration.getStatisticType().toString());
            return false;
        } else if (!statisticConfiguration.isEnabled()) {
            LOGGER.warn(
                    "The StatisticDataSource with name {}, engine {} and type {} is not enabled, so no statistics will be recorded for it.",
                    statisticName, engineName, statisticConfiguration.getStatisticType().toString());
            return false;
        }

        return true;
    }
}
