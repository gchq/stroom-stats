

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

import com.google.common.base.Preconditions;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SearchStatisticsCriteria {

    private final Period period;
    private final String statisticName;
    private final FilterTermsTree filterTermsTree;
    private final Set<String> rolledUpFieldNames;
    private final List<String> requiredDynamicFields;
    private final EventStoreTimeIntervalEnum interval;

    private SearchStatisticsCriteria(final Period period,
                                     final String statisticName,
                                     final FilterTermsTree filterTermsTree,
                                     final Set<String> rolledUpFieldNames,
                                     final List<String> requiredDynamicFields,
                                     final EventStoreTimeIntervalEnum interval) {
        this.period = period;
        this.statisticName = statisticName;
        this.filterTermsTree = filterTermsTree;
        this.rolledUpFieldNames = rolledUpFieldNames;
        this.requiredDynamicFields = requiredDynamicFields;
        this.interval = interval;
    }

    public Period getPeriod() {
        return period;
    }

    public String getStatisticName() {
        return statisticName;
    }

    public Optional<EventStoreTimeIntervalEnum> getInterval() {
        return Optional.ofNullable(interval);
    }

    /**
     * @return A list names of fields that have a roll up operation applied to
     * them
     */
    public Set<String> getRolledUpFieldNames() {
        return rolledUpFieldNames;
    }


    public List<String> getRequiredDynamicFields() {
        return requiredDynamicFields;
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
        return "SearchStatisticsCriteria{" +
                "period=" + period +
                ", statisticName='" + statisticName + '\'' +
                ", filterTermsTree=" + filterTermsTree +
                ", rolledUpFieldNames=" + rolledUpFieldNames +
                ", interval=" + interval +
                '}';
    }

    public static SearchStatisticsCriteriaBuilder builder(final Period period, final String statisticName) {
        return new SearchStatisticsCriteriaBuilder(period, statisticName);
    }

    public static class SearchStatisticsCriteriaBuilder {
        private Period period;
        private String statisticName;
        private FilterTermsTree filterTermsTree = FilterTermsTree.emptyTree();
        private Set<String> rolledUpFieldNames = Collections.emptySet();
        private List<String> requiredDynamicFields = Collections.emptyList();
        private EventStoreTimeIntervalEnum interval = null;

        SearchStatisticsCriteriaBuilder(final Period period, final String statisticName) {
            Preconditions.checkNotNull(period);
            Preconditions.checkNotNull(statisticName);
            this.period = period;
            this.statisticName = statisticName;
        }

        public SearchStatisticsCriteriaBuilder setFilterTermsTree(final FilterTermsTree filterTermsTree) {
            Preconditions.checkNotNull(filterTermsTree);
            this.filterTermsTree = filterTermsTree;
            return this;
        }

        public SearchStatisticsCriteriaBuilder setRolledUpFieldNames(final Set<String> rolledUpFieldNames) {
            Preconditions.checkNotNull(rolledUpFieldNames);
            this.rolledUpFieldNames = rolledUpFieldNames;
            return this;
        }

        public SearchStatisticsCriteriaBuilder setRequiredDynamicFields(final List<String> requiredDynamicFields) {
            Preconditions.checkNotNull(requiredDynamicFields);
            this.requiredDynamicFields = requiredDynamicFields;
            return this;
        }

        public SearchStatisticsCriteriaBuilder setInterval(final EventStoreTimeIntervalEnum interval) {
            Preconditions.checkNotNull(interval);
            this.interval = interval;
            return this;
        }

        public SearchStatisticsCriteria build() {
            return new SearchStatisticsCriteria(period, statisticName, filterTermsTree, rolledUpFieldNames, requiredDynamicFields, interval);
        }
    }
}
