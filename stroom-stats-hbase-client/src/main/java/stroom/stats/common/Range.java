

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

import java.io.Serializable;


/**
 * Class that holds a range of some number type. A null upper or lower bound
 * means an open ended range. The upper bound is not included i.e. [0..10) means
 * 0,1,2,3,4,5,6,7,8,9 or this can be represented by the toString [0..9]
 */
public class Range<T extends Number> implements Serializable, HasIsConstrained {

    private static final long serialVersionUID = -7056532720481684193L;

    private T from;
    private T to;
    private boolean matchNull = false;

    public Range() {
        init(null, null);
    }

    public Range(final T from, final T to) {
        init(from, to);
    }

    protected void init(final T from, final T to) {
        this.from = from;
        this.to = to;
    }

    public void setMatchNull(final boolean matchNull) {
        this.matchNull = matchNull;
    }

    public boolean isMatchNull() {
        return matchNull;
    }

    /**
     * Does this range contain this number?
     */
    public boolean contains(final long num) {
        // If we have a lower bound check that the time is not before it.
        if (from != null && num < from.longValue()) {
            return false;
        }
        // If we have an upper bound check that the time is not ON or after it.
        if (to != null && num >= to.longValue()) {
            return false;
        }

        // Must be covered in our period then
        return true;
    }

    /**
     * Is this period after a time?
     */
    public boolean after(final long num) {
        return from != null && from.longValue() > num;
    }

    /**
     * Is this period before a time?
     */
    public boolean before(final long num) {
        return to != null && to.longValue() < num;
    }

    public T getFrom() {
        return from;
    }

    /**
     * @param other
     *            value to return if from is null
     * @return The from value or if that is null, the supplied other value
     */
    public T getFromOrElse(final T other) {
        return from != null ? from : other;
    }

    protected void setFrom(final T from) {
        this.from = from;
    }

    public T getTo() {
        return to;
    }

    /**
     * @param other
     *            value to return if to is null
     * @return The to value or if that is null, the supplied other value
     */
    public T getToOrElse(final T other) {
        return to != null ? to : other;
    }

    protected void setTo(final T to) {
        this.to = to;
    }

    /**
     * @return have we an upper and lower bound?
     */
    public boolean isBounded() {
        return from != null && to != null;
    }

    /**
     * Are we empty ? i.e. the lower bound is the same as the upper one.
     */
    public boolean isEmpty() {
        return isBounded() && from.longValue() >= to.longValue();
    }

    public Number size() {
        if (!isBounded()) {
            return null;
        }
        return getTo().longValue() - getFrom().longValue();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Range<T> union(final Range other) {
        return new Range(this.from.longValue() < other.from.longValue() ? this.from : other.from,
                this.to.longValue() > other.to.longValue() ? this.to : other.to);

    }

    /**
     * @return have we an upper or lower bound or we are to just match null?
     */
    @Override
    public boolean isConstrained() {
        return from != null || to != null || matchNull;
    }


    /**
     * Determine if a supplied time is within this period. Used for mock stream
     * store.
     */
    public boolean isMatch(final T num) {
        if (isConstrained()) {
            return true;
        }
        if (from == null) {
            return num.longValue() < to.longValue();
        }
        if (to == null) {
            return num.longValue() >= from.longValue();
        }
        return num.longValue() >= from.longValue() && num.longValue() < to.longValue();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (from != null) {
            sb.append("[");
            sb.append(from);
        }
        sb.append("..");
        if (to != null) {
            sb.append(to.longValue());
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Range<?> range = (Range<?>) o;

        if (matchNull != range.matchNull) return false;
        if (from != null ? !from.equals(range.from) : range.from != null) return false;
        return to != null ? to.equals(range.to) : range.to == null;
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (matchNull ? 1 : 0);
        return result;
    }
}
