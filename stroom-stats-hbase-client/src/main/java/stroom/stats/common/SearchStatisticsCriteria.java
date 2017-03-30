

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
import java.util.Optional;
import java.util.Set;

public class SearchStatisticsCriteria {

    private final Period period;
    private final String statisticName;
    private final FilterTermsTree filterTermsTree;
    private final Set<String> rolledUpFieldNames;
    private final EventStoreTimeIntervalEnum interval;

    private SearchStatisticsCriteria(final Period period,
                                     final String statisticName,
                                     final FilterTermsTree filterTermsTree,
                                     final Set<String> rolledUpFieldNames,
                                     final EventStoreTimeIntervalEnum interval) {
        this.period = period;
        this.statisticName = statisticName;
        this.filterTermsTree = filterTermsTree;
        this.rolledUpFieldNames = rolledUpFieldNames;
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

    public static FindEventCriteriaBuilder builder(final Period period, final String statisticName) {
        return new FindEventCriteriaBuilder(period, statisticName);
    }

    public static class FindEventCriteriaBuilder {
        private Period period;
        private String statisticName;
        private FilterTermsTree filterTermsTree = FilterTermsTree.emptyTree();
        private Set<String> rolledUpFieldNames = Collections.emptySet();
        private EventStoreTimeIntervalEnum interval = null;

        FindEventCriteriaBuilder(final Period period, final String statisticName) {
            Preconditions.checkNotNull(period);
            Preconditions.checkNotNull(statisticName);
            this.period = period;
            this.statisticName = statisticName;
        }

        public FindEventCriteriaBuilder setFilterTermsTree(final FilterTermsTree filterTermsTree) {
            Preconditions.checkNotNull(filterTermsTree);
            this.filterTermsTree = filterTermsTree;
            return this;
        }

        public FindEventCriteriaBuilder setRolledUpFieldNames(final Set<String> rolledUpFieldNames) {
            Preconditions.checkNotNull(rolledUpFieldNames);
            this.rolledUpFieldNames = rolledUpFieldNames;
            return this;
        }

        public void setInterval(final EventStoreTimeIntervalEnum interval) {
            Preconditions.checkNotNull(interval);
            this.interval = interval;
        }

        public SearchStatisticsCriteria build() {
            return new SearchStatisticsCriteria(period, statisticName, filterTermsTree, rolledUpFieldNames, interval);
        }
    }
}
