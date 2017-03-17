

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
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractCustomRollUpMask implements CustomRollUpMask {
    /**
     * Holds a list of the positions of tags that are rolled up, zero based. The
     * position number is based on the alphanumeric sorted list of tag/field
     * names in the {@link StatisticConfiguration}. Would use a SortedSet but that
     * is not supported by GWT. Must ensure the contents of this are sorted so
     * that when contains is called on lists of these objects it works
     * correctly.
     */
    private List<Integer> rolledUpTagPositions = new ArrayList<>();

    AbstractCustomRollUpMask() {
    }

    public AbstractCustomRollUpMask(final List<Integer> rolledUpTagPositions) {
        this.rolledUpTagPositions = rolledUpTagPositions.stream().sorted().collect(Collectors.toList());
    }


    @Override
    public List<Integer> getRolledUpTagPositions() {
        return rolledUpTagPositions;
    }

    public void setRolledUpTagPositions(final List<Integer> rolledUpTagPositions) {
        this.rolledUpTagPositions = rolledUpTagPositions.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public boolean isTagRolledUp(final int position) {
        return rolledUpTagPositions.contains(position);
    }

    public void setRollUpState(final Integer position, final boolean isRolledUp) {
        if (isRolledUp) {
            if (!rolledUpTagPositions.contains(position)) {
                rolledUpTagPositions.add(position);
                Collections.sort(this.rolledUpTagPositions);
            }
        } else {
            if (rolledUpTagPositions.contains(position)) {
                rolledUpTagPositions.remove(position);
                // no need to re-sort on remove as already in order
            }
        }

    }

    public List<Integer> deepCloneTagPositions() {
        final List<Integer> tagPositions = new ArrayList<>();
        for (final Integer tagPosition : tagPositions) {
            tagPositions.add(new Integer(tagPosition));
        }
        return tagPositions;
    }


    @Override
    public abstract CustomRollUpMask deepCopy();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((rolledUpTagPositions == null) ? 0 : rolledUpTagPositions.hashCode());
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
        final AbstractCustomRollUpMask other = (AbstractCustomRollUpMask) obj;
        if (rolledUpTagPositions == null) {
            if (other.rolledUpTagPositions != null)
                return false;
        } else if (!rolledUpTagPositions.equals(other.rolledUpTagPositions))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CustomRollUpMask [rolledUpTagPositions=" + rolledUpTagPositions + "]";
    }
}
