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

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

/**
 * <p>
 * Used to hold system wide groups in the application.
 * </p>
 */
@Entity
@Table(name = "FOLDER", uniqueConstraints = @UniqueConstraint(columnNames = {"FK_FOLDER_ID", "NAME"}))
public class Folder extends DocumentEntity implements Copyable<Folder> {
    public static final String TABLE_NAME = SQLNameConstants.FOLDER;
    public static final String FOREIGN_KEY = FK_PREFIX + TABLE_NAME + ID_SUFFIX;
    public static final String ENTITY_TYPE = "Folder";

    private static final long serialVersionUID = -4208920620555926044L;

    public static Folder create(final Folder parent, final String name) {
        final Folder folder = new Folder();
        folder.setFolder(parent);
        folder.setName(name);
        return folder;
    }

    public static final Folder createStub(final long pk) {
        final Folder folder = new Folder();
        folder.setStub(pk);
        return folder;
    }

    @Override
    public void copyFrom(final Folder other) {
        super.copyFrom(other);
    }

    @Transient
    @Override
    public final String getType() {
        return ENTITY_TYPE;
    }
}
