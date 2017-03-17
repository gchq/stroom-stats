

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import stroom.stats.configuration.common.SharedObject;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "data")
public class StatisticConfigurationEntityData implements SharedObject {
    private static final long serialVersionUID = -9071682094300037627L;

    /**
     * Should be a SortedSet but GWT doesn't support that. Contents should be
     * sorted and not contain duplicates
     *
     * XMLTransient to force JAXB to use the setter
     */

    @XmlTransient
    private List<StatisticField> statisticFields;

    /**
     * Held in a set to prevent duplicates.
     *
     * XMLTransient to force JAXB to use the setter
     */
    @XmlTransient
    private Set<CustomRollUpMaskEntityObject> customRollUpMasks;

    // cache the positions of the
    @XmlTransient
    private Map<String, Integer> fieldPositionMap = new HashMap<>();

    public StatisticConfigurationEntityData() {
        this(new ArrayList<StatisticField>(), new HashSet<>());
    }

    public StatisticConfigurationEntityData(final List<StatisticField> statisticFields) {
        this(new ArrayList<StatisticField>(statisticFields), new HashSet<CustomRollUpMaskEntityObject>());
    }

    public StatisticConfigurationEntityData(final List<StatisticField> statisticFields,
                                            final Set<CustomRollUpMaskEntityObject> customRollUpMasks) {
        this.statisticFields = statisticFields;
        this.customRollUpMasks = customRollUpMasks;

        // sort the list of fields as this will help us later when generating
        // StatisticEvents
        sortFieldListAndCachePositions();
    }

    @XmlElement(name = "field")
    public List<StatisticField> getStatisticFields() {
        return statisticFields;
    }

    public void setStatisticFields(final List<StatisticField> statisticFields) {
        this.statisticFields = statisticFields;
        sortFieldListAndCachePositions();
    }

    @XmlElement(name = "customRollUpMask")
    public Set<CustomRollUpMaskEntityObject> getCustomRollUpMasks() {
        return customRollUpMasks;
    }

    public void setCustomRollUpMasks(final Set<CustomRollUpMaskEntityObject> customRollUpMasks) {
        this.customRollUpMasks = customRollUpMasks;
    }

    public void addStatisticField(final StatisticField statisticField) {
        if (statisticFields == null) {
            statisticFields = new ArrayList<>();
        }
        // prevent duplicates
        if (!statisticFields.contains(statisticField)) {
            statisticFields.add(statisticField);
            sortFieldListAndCachePositions();
        }
    }

    public void removeStatisticField(final StatisticField statisticField) {
        if (statisticFields != null) {
            statisticFields.remove(statisticField);
            sortFieldListAndCachePositions();
        }
    }

    public void reOrderStatisticFields() {
        if (statisticFields != null) {
            sortFieldListAndCachePositions();
        }
    }

    public boolean containsStatisticField(final StatisticField statisticField) {
        if (statisticFields != null) {
            return statisticFields.contains(statisticField);
        }
        return false;
    }

    public void addCustomRollUpMask(final CustomRollUpMaskEntityObject customRollUpMask) {
        if (customRollUpMasks == null) {
            customRollUpMasks = new HashSet<>();
        }

        customRollUpMasks.add(customRollUpMask);
    }

    public void removeCustomRollUpMask(final CustomRollUpMask customRollUpMask) {
        if (customRollUpMasks != null) {
            customRollUpMasks.remove(customRollUpMask);
        }
    }

    public void clearCustomRollUpMask() {
        if (customRollUpMasks != null) {
            customRollUpMasks.clear();
        }
    }

    public boolean containsCustomRollUpMask(final CustomRollUpMask customRollUpMask) {
        if (customRollUpMasks != null) {
            return customRollUpMasks.contains(customRollUpMask);
        }
        return false;
    }

    public boolean isRollUpCombinationSupported(final Set<String> rolledUpFieldNames) {
        if (rolledUpFieldNames == null || rolledUpFieldNames.isEmpty()) {
            return true;
        }

        if (rolledUpFieldNames.size() > statisticFields.size()) {
            throw new RuntimeException(
                    "isRollUpCombinationSupported called with more rolled up fields (" + rolledUpFieldNames.toString()
                            + ") than there are statistic fields (" + fieldPositionMap.keySet() + ")");
        }

        if (!fieldPositionMap.keySet().containsAll(rolledUpFieldNames)) {
            throw new RuntimeException(
                    "isRollUpCombinationSupported called rolled up fields (" + rolledUpFieldNames.toString()
                            + ") that don't exist in the statistic fields list (" + fieldPositionMap.keySet() + ")");
        }

        final List<Integer> rolledUpFieldPositions = new ArrayList<>();
        for (final String rolledUpField : rolledUpFieldNames) {
            rolledUpFieldPositions.add(getFieldPositionInList(rolledUpField));
        }

        return customRollUpMasks.contains(new CustomRollUpMaskEntityObject(rolledUpFieldPositions));
    }

    public Integer getFieldPositionInList(final String fieldName) {
        return fieldPositionMap.get(fieldName);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((statisticFields == null) ? 0 : statisticFields.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final StatisticConfigurationEntityData other = (StatisticConfigurationEntityData) obj;
        if (statisticFields == null) {
            if (other.statisticFields != null)
                return false;
        } else if (!statisticFields.equals(other.statisticFields))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "StatisticFields [statisticFields=" + statisticFields + "]";
    }

    private void sortFieldListAndCachePositions() {
        // de-dup the list
        Set<StatisticField> tempSet = new HashSet<>(statisticFields);
        statisticFields.clear();
        statisticFields.addAll(tempSet);
        tempSet = null;

        Collections.sort(statisticFields);

        fieldPositionMap.clear();
        int i = 0;
        for (final StatisticField field : statisticFields) {
            fieldPositionMap.put(field.getFieldName(), i++);
        }
    }

    public StatisticConfigurationEntityData deepCopy() {
        final List<StatisticField> newFieldList = new ArrayList<>();

        for (final StatisticField statisticField : statisticFields) {
            newFieldList.add(statisticField.deepCopy());
        }

        final Set<CustomRollUpMaskEntityObject> newMaskList = new HashSet<>();

        for (final CustomRollUpMaskEntityObject customRollUpMask : customRollUpMasks) {
            newMaskList.add((CustomRollUpMaskEntityObject) customRollUpMask.deepCopy());
        }

        return new StatisticConfigurationEntityData(newFieldList, newMaskList);
    }

    /**
     * Added to ensure map is not made final which would break GWT
     * serialisation.
     */
    public void setFieldPositionMap(final Map<String, Integer> fieldPositionMap) {
        this.fieldPositionMap = fieldPositionMap;
    }
}
