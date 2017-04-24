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

import stroom.stats.api.StatisticType;

import java.util.*;
import java.util.stream.Collectors;

public class MockStatisticConfiguration implements StatisticConfiguration {

    private String name;
    private String uuid;
    private String description;
    private String engineName;
    private StatisticType statisticType;
    private StatisticRollUpType rollUpType;
    private Long precision;
    private List<String> fieldNames = new ArrayList<>();
    private Set<CustomRollUpMask> customRollUpMasks = new HashSet<>();

    public MockStatisticConfiguration() {
        uuid = UUID.randomUUID().toString();
    }

    public MockStatisticConfiguration(final String name, final StatisticType statisticType,
                                      final StatisticRollUpType rollUpType, final Long precision,
                                      final Set<CustomRollUpMask> customRollUpMasks,
                                      final String... fieldNames) {
        this.name = name;
        this.statisticType = statisticType;
        this.rollUpType = rollUpType;
        this.precision = precision;
        this.customRollUpMasks = customRollUpMasks;
        this.fieldNames = Arrays.asList(fieldNames);
    }

    public MockStatisticConfiguration setName(final String name) {
        this.name = name;
        return this;
    }

    public MockStatisticConfiguration setUuid(final String uuid) {
        this.uuid = uuid;
        return this;
    }

    public MockStatisticConfiguration setDescription(final String description) {
        this.description = description;
        return this;
    }

    public MockStatisticConfiguration setEngineName(final String engineName) {
        this.engineName = engineName;
        return this;
    }

    public MockStatisticConfiguration setStatisticType(final StatisticType statisticType) {
        this.statisticType = statisticType;
        return this;
    }

    public MockStatisticConfiguration setRollUpType(final StatisticRollUpType rollUpType) {
        this.rollUpType = rollUpType;
        return this;
    }

    public MockStatisticConfiguration setPrecision(final Long precision) {
        this.precision = precision;
        return this;
    }

    public MockStatisticConfiguration addFieldName(String fieldName) {
        fieldNames.add(fieldName);
        return this;
    }

    public MockStatisticConfiguration addFieldNames(Collection<String> fieldNames) {
        this.fieldNames.addAll(fieldNames);
        return this;
    }

    public MockStatisticConfiguration addFieldNames(String... fieldNames) {
        for (final String fieldName : fieldNames) {
            this.fieldNames.add(fieldName);
        }
        return this;
    }

    public MockStatisticConfiguration addCustomRollupMask(CustomRollUpMask customRollUpMask) {
        customRollUpMasks.add(customRollUpMask);
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getEngineName() {
        return engineName;
    }

    @Override
    public StatisticType getStatisticType() {
        return statisticType;
    }

    @Override
    public StatisticRollUpType getRollUpType() {
        return rollUpType;
    }

    @Override
    public Long getPrecision() {
        return precision;
    }

    @Override
    public boolean isEnabled() {
        throw new UnsupportedOperationException("Not supported in this implementation");
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public Set<? extends CustomRollUpMask> getCustomRollUpMasks() {
        return customRollUpMasks;
    }

    @Override
    public Integer getPositionInFieldList(final String fieldName) {
        return getFieldNames().indexOf(fieldName);
    }

    @Override
    public boolean isRollUpCombinationSupported(final Set<String> rolledUpFieldNames) {
        throw new UnsupportedOperationException("Not yet supported in this implementation");
    }

    @Override
    public boolean isValidField(final String fieldName) {
        throw new UnsupportedOperationException("Not yet supported in this implementation");
    }
}
