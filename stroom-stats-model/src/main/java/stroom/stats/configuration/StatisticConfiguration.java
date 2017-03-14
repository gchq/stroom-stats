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

package stroom.stats.configuration;

import stroom.stats.api.StatisticType;

import java.util.List;
import java.util.Set;


/**
 * Represents the definition of a statistic data set, i.e. the stat name, type
 * finest precision and tag names
 */
public interface StatisticConfiguration {
    String ENTITY_TYPE = "StatisticConfiguration";
    String ENTITY_TYPE_FOR_DISPLAY = "Statistic Store";

    //static field names
    String FIELD_NAME_DATE_TIME = "Date Time";
    String FIELD_NAME_VALUE = "Statistic Value";
    String FIELD_NAME_COUNT = "Statistic Count";
    String FIELD_NAME_MIN_VALUE = "Min Statistic Value";
    String FIELD_NAME_MAX_VALUE = "Max Statistic Value";
    String FIELD_NAME_PRECISION = "Precision";
    String FIELD_NAME_PRECISION_MS = "Precision ms";

    String getName();

    default String getType() {
        return ENTITY_TYPE;
    }

    String getUuid();

    String getDescription();

    String getEngineName();

    StatisticType getStatisticType();

    StatisticRollUpType getRollUpType();

    Long getPrecision();

    boolean isEnabled();

    /**
     * All the field names (aka tags) for this statistic, in alphanumeric order
     */
    List<String> getFieldNames();

    Set<? extends CustomRollUpMask> getCustomRollUpMasks();

    /**
     * The position of the passed fieldName in the output of getFieldNames,
     * e.g. if getFieldNames returns TagA,TagB,TagC then
     * getPositionInFieldList for TagB will return 1
     * @return Zero based position in sorted field name list
     */
    Integer getPositionInFieldList(final String fieldName);

    boolean isRollUpCombinationSupported(final Set<String> rolledUpFieldNames);

    boolean isValidField(final String fieldName);



}
