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

package stroom.stats.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import stroom.stats.api.StatisticType;

import java.util.*;


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

    //Define the static fields available to each type of statistic
    Map<StatisticType, List<String>> STATIC_FIELDS_MAP = Maps.immutableEnumMap(ImmutableMap.of(
            StatisticType.COUNT, Arrays.asList(
                    FIELD_NAME_DATE_TIME,
                    FIELD_NAME_COUNT,
                    FIELD_NAME_PRECISION,
                    FIELD_NAME_PRECISION_MS
            ),
            StatisticType.VALUE, Arrays.asList(
                    FIELD_NAME_DATE_TIME,
                    FIELD_NAME_VALUE,
                    FIELD_NAME_COUNT,
                    FIELD_NAME_MIN_VALUE,
                    FIELD_NAME_MAX_VALUE,
                    FIELD_NAME_PRECISION,
                    FIELD_NAME_PRECISION_MS
            )
    ));

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
     * @return All the dynamic field names (aka tags) for this statistic, in alphanumeric order
     */
    List<String> getFieldNames();

    /**
     * @return All fields, static and dynamic, for this statistic taking into account the type of the statistic
     */
    default List<String> getAllFieldNames() {
        List<String> allFieldNames = new ArrayList<>(STATIC_FIELDS_MAP.get(getStatisticType()));
        allFieldNames.addAll(getFieldNames());
        return allFieldNames;
    }

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
