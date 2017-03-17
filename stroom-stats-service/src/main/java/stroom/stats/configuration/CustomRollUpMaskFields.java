

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

import stroom.stats.configuration.common.SharedObject;

import java.util.Set;


@Deprecated //Doesn't seem to be used anywhere
public class CustomRollUpMaskFields implements SharedObject, Comparable<CustomRollUpMaskFields> {
    private static final long serialVersionUID = 5490835313079322510L;

    private int id;
    private short maskValue;
    private Set<Integer> rolledUpFieldPositions;

    public CustomRollUpMaskFields() {
        // Default constructor necessary for GWT serialisation.
    }

    public CustomRollUpMaskFields(final int id, final short maskValue, final Set<Integer> rolledUpFieldPositions) {
        this.id = id;
        this.maskValue = maskValue;
        this.rolledUpFieldPositions = rolledUpFieldPositions;
    }

    public int getId() {
        return id;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public short getMaskValue() {
        return maskValue;
    }

    public void setMaskValue(final short maskValue) {
        this.maskValue = maskValue;
    }

    public Set<Integer> getRolledUpFieldPositions() {
        return rolledUpFieldPositions;
    }

    public void setRolledUpFieldPositions(final Set<Integer> rolledUpFieldPositions) {
        this.rolledUpFieldPositions = rolledUpFieldPositions;
    }

    public boolean isFieldRolledUp(final int position) {
        if (rolledUpFieldPositions != null) {
            return rolledUpFieldPositions.contains(position);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "CustomRollUpMaskFields [maskValue=" + maskValue + ", rolledUpFieldPositions=" + rolledUpFieldPositions
                + "]";
    }

    @Override
    public int compareTo(final CustomRollUpMaskFields that) {
        return Short.valueOf(this.maskValue).compareTo(that.maskValue);
    }
}
