

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

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;
import javax.validation.constraints.Size;

@MappedSuperclass
public abstract class NamedEntity extends AuditedEntity implements HasName, HasDisplayValue {
    private static final long serialVersionUID = -6752797140242673318L;

    public static final String NAME = SQLNameConstants.NAME;

    private String name;

    @Override
    @Column(name = NAME, nullable = false)
    @Size(min = LengthConstants.MIN_NAME_LENGTH)
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Transient
    @Override
    public String getDisplayValue() {
        return String.valueOf(getName());
    }

    protected void copyFrom(final NamedEntity t) {
        this.name = t.name;
    }

    @Override
    protected void toString(final StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=");
        sb.append(name);
    }
}
