

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

package stroom.stats.configuration.common;

/**
 * A GWT friendly version of commons EqualsBuilder.
 */
public class EqualsBuilder {
    private boolean isEquals = true;

    public EqualsBuilder appendSuper(final boolean value) {
        if (!isEquals) {
            return this;
        }
        isEquals = value;
        return this;
    }

    public EqualsBuilder append(final Object lhs, final Object rhs) {
        if (!isEquals) {
            return this;
        }
        if (lhs == rhs) {
            return this;
        }
        if (lhs == null || rhs == null) {
            isEquals = false;
            return this;
        }
        isEquals = lhs.equals(rhs);
        return this;
    }

    public EqualsBuilder append(final long lhs, final long rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final int lhs, final int rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final short lhs, final short rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final char lhs, final char rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final byte lhs, final byte rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final double lhs, final double rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final float lhs, final float rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public EqualsBuilder append(final boolean lhs, final boolean rhs) {
        if (!isEquals) {
            return this;
        }

        isEquals = (lhs == rhs);
        return this;
    }

    public boolean isEquals() {
        return isEquals;
    }
}
