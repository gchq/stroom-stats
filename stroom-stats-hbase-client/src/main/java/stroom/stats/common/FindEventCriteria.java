

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

package stroom.stats.common;

import java.util.Collections;
import java.util.Set;

public class FindEventCriteria {

    private final Period period;
    private final String statisticName;
    private final FilterTermsTree filterTermsTree;
    private final Set<String> rolledUpFieldNames;

    private FindEventCriteria(final Period period, final String statisticName, final FilterTermsTree filterTermsTree,
                      final Set<String> rolledUpFieldNames) {
        this.period = period;
        this.statisticName = statisticName;
        this.filterTermsTree = filterTermsTree;
        this.rolledUpFieldNames = rolledUpFieldNames;
    }

    public Period getPeriod() {
        return period;
    }

    public String getStatisticName() {
        return statisticName;
    }

    /**
     * @return A list names of fields that have a roll up operation applied to
     *         them
     */
    public Set<String> getRolledUpFieldNames() {
        return rolledUpFieldNames;
    }

    /**
     * Return the {@link FilterTermsTree} object on this criteria
     *
     * @return The filter tree, may be null if not filter is defined.
     */
    public FilterTermsTree getFilterTermsTree() {
        return this.filterTermsTree;
    }

    @Override
    public String toString() {
        return "FindEventCriteria [period=" + period + ", statisticName=" + statisticName + ", filterTermsTree="
                + filterTermsTree + ", rolledUpFieldNames=" + rolledUpFieldNames + "]";
    }

    public static FindEventCriteriaBuilder builder(final Period period, final String statisticName) {
        return new FindEventCriteriaBuilder(period, statisticName);
    }

    public static class FindEventCriteriaBuilder {
        private Period period;
        private String statisticName;
        private FilterTermsTree filterTermsTree = FilterTermsTree.emptyTree();
        private Set<String> rolledUpFieldNames = Collections.emptySet();

        FindEventCriteriaBuilder(final Period period, final String statisticName) {
            this.period = period;
            this.statisticName = statisticName;
        }


        public FindEventCriteriaBuilder setFilterTermsTree(final FilterTermsTree filterTermsTree) {
            this.filterTermsTree = filterTermsTree;
            return this;
        }

        public FindEventCriteriaBuilder setRolledUpFieldNames(final Set<String> rolledUpFieldNames) {
            this.rolledUpFieldNames = rolledUpFieldNames;
            return this;
        }

        public FindEventCriteria build() {
            return new FindEventCriteria(period, statisticName, filterTermsTree, rolledUpFieldNames);
        }
    }
}
