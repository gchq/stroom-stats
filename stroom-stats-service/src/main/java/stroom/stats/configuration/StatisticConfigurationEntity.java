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
import stroom.stats.configuration.common.DocumentEntity;
import stroom.stats.configuration.common.ExternalFile;
import stroom.stats.configuration.common.SQLNameConstants;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;
import javax.xml.bind.annotation.XmlTransient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Entity
@Table(name = "STAT_DAT_SRC", uniqueConstraints = @UniqueConstraint(columnNames = { "NAME", "ENGINE_NAME" }) )
public class StatisticConfigurationEntity extends DocumentEntity implements StatisticConfiguration {
    // Hibernate table/column names
    public static final String TABLE_NAME = SQLNameConstants.STATISTIC + SEP + SQLNameConstants.DATA + SEP
            + SQLNameConstants.SOURCE;
    public static final String ENGINE_NAME = SQLNameConstants.ENGINE + SEP + SQLNameConstants.NAME;
    public static final String STATISTIC_TYPE = SQLNameConstants.STATISTIC + SEP + SQLNameConstants.TYPE;
    public static final String PRECISION = SQLNameConstants.PRECISION;
    public static final String ROLLUP_TYPE = SQLNameConstants.ROLLUP + SEP + SQLNameConstants.TYPE;
    public static final String FOREIGN_KEY = FK_PREFIX + TABLE_NAME + ID_SUFFIX;
    public static final String NOT_SET = "NOT SET";
    public static final Long DEFAULT_PRECISION = EventStoreTimeIntervalEnum.HOUR.columnInterval();
    public static final String DEFAULT_NAME_PATTERN_VALUE = "^[a-zA-Z0-9_\\- \\.\\(\\)]{1,}$";

    private static final long serialVersionUID = -649286188919707915L;

    private String description;
    private String engineName;
    private byte pStatisticType;
    private byte pRollUpType;
    private Long precision;
    private boolean enabled = false;

    private String data;
    private StatisticConfigurationEntityData statisticConfigurationEntityDataObject;

    public StatisticConfigurationEntity() {
        setDefaults();
    }

    private void setDefaults() {
        this.pStatisticType = StatisticType.COUNT.getPrimitiveValue();
        this.pRollUpType = StatisticRollUpType.NONE.getPrimitiveValue();
        this.engineName = NOT_SET;
        this.precision = DEFAULT_PRECISION;
    }

    @Override
    @Column(name = SQLNameConstants.DESCRIPTION)
    @Lob
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    @Transient
    public String getType() {
        return StatisticConfiguration.super.getType();
    }

    @Override
    @Column(name = ENGINE_NAME, nullable = false)
    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(final String engineName) {
        this.engineName = engineName;
    }

    @Column(name = STATISTIC_TYPE, nullable = false)
    public byte getpStatisticType() {
        return pStatisticType;
    }

    public void setpStatisticType(final byte pStatisticType) {
        this.pStatisticType = pStatisticType;
    }

    @Override
    @Transient
    public StatisticType getStatisticType() {
        return StatisticType.PRIMITIVE_VALUE_CONVERTER.fromPrimitiveValue(pStatisticType);
    }

    public void setStatisticType(final StatisticType statisticType) {
        this.pStatisticType = statisticType.getPrimitiveValue();
    }

    @Column(name = ROLLUP_TYPE, nullable = false)
    public byte getpRollUpType() {
        return pRollUpType;
    }

    public void setpRollUpType(final byte pRollUpType) {
        this.pRollUpType = pRollUpType;
    }

    @Override
    @Transient
    public StatisticRollUpType getRollUpType() {
        return StatisticRollUpType.PRIMITIVE_VALUE_CONVERTER.fromPrimitiveValue(pRollUpType);
    }

    public void setRollUpType(final StatisticRollUpType rollUpType) {
        this.pRollUpType = rollUpType.getPrimitiveValue();
    }

    @Override
    @Column(name = PRECISION, nullable = false)
    public Long getPrecision() {
        return precision;
    }

    public void setPrecision(final Long precision) {
        this.precision = precision;
    }

    @Override
    @Column(name = SQLNameConstants.ENABLED, nullable = false)
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    @Lob
    @Column(name = SQLNameConstants.DATA, length = Integer.MAX_VALUE)
    @ExternalFile
    public String getData() {
        return data;
    }

    public void setData(final String data) {
        this.data = data;
    }

    @Transient
    @XmlTransient
    public StatisticConfigurationEntityData getStatisticDataSourceDataObject() {
        return statisticConfigurationEntityDataObject;
    }

    public void setStatisticDataSourceDataObject(final StatisticConfigurationEntityData statisticDataSourceDataObject) {
        this.statisticConfigurationEntityDataObject = statisticDataSourceDataObject;
    }

    @Override
    @Transient
    public boolean isValidField(final String fieldName) {
        if (statisticConfigurationEntityDataObject == null) {
            return false;
        } else if (statisticConfigurationEntityDataObject.getStatisticFields() == null) {
            return false;
        } else {
            return statisticConfigurationEntityDataObject.getStatisticFields().contains(new StatisticField(fieldName));
        }
    }

    @Override
    @Transient
    public boolean isRollUpCombinationSupported(final Set<String> rolledUpFieldNames) {
        if (rolledUpFieldNames == null || rolledUpFieldNames.isEmpty()) {
            return true;
        }

        if (!rolledUpFieldNames.isEmpty() && getRollUpType().equals(StatisticRollUpType.NONE)) {
            return false;
        }

        if (getRollUpType().equals(StatisticRollUpType.ALL)) {
            return true;
        }

        // rolledUpFieldNames not empty if we get here

        if (statisticConfigurationEntityDataObject == null) {
            throw new RuntimeException(
                    "isRollUpCombinationSupported called with non-empty list but data source has no statistic fields or custom roll up masks");
        }

        return statisticConfigurationEntityDataObject.isRollUpCombinationSupported(rolledUpFieldNames);
    }

    @Override
    @Transient
    public Integer getPositionInFieldList(final String fieldName) {
        return statisticConfigurationEntityDataObject.getFieldPositionInList(fieldName);
    }

    @Override
    @Transient
    public List<String> getFieldNames() {
        if (statisticConfigurationEntityDataObject != null) {
            final List<String> fieldNames = new ArrayList<>();
            for (final StatisticField statisticField : statisticConfigurationEntityDataObject.getStatisticFields()) {
                fieldNames.add(statisticField.getFieldName());
            }
            return fieldNames;
        } else {
            return Collections.<String> emptyList();
        }
    }

    @Transient
    public int getStatisticFieldCount() {
        return statisticConfigurationEntityDataObject == null ? 0 : statisticConfigurationEntityDataObject.getStatisticFields().size();
    }

    @Transient
    public List<StatisticField> getStatisticFields() {
        if (statisticConfigurationEntityDataObject != null) {
            return statisticConfigurationEntityDataObject.getStatisticFields();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    @Transient
    public Set<? extends CustomRollUpMask> getCustomRollUpMasks() {
        if (statisticConfigurationEntityDataObject != null) {
            return statisticConfigurationEntityDataObject.getCustomRollUpMasks();
        } else {
            return Collections.emptySet();
        }
    }
}
