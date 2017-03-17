

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

/**
 * Modified version of Stroom's DocumentEntity, removing the folder member variable
 * as the concept of a folder has no meaning in stroom-stats
 */
@MappedSuperclass
public abstract class DocumentEntity extends NamedEntity implements Document {
    public static final String UUID = SQLNameConstants.UUID;

    private static final long serialVersionUID = -6752797140242673318L;

    private String uuid;

    @Override
    @Column(name = UUID, unique = true, nullable = false)
    public String getUuid() {
        return uuid;
    }

    public void setUuid(final String uuid) {
        this.uuid = uuid;
    }

    @Override
    public void clearPersistence() {
        super.clearPersistence();
        uuid = null;
    }
}
