

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

import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

@MappedSuperclass
public abstract class Entity implements HasType, SharedObject {
    private static final long serialVersionUID = 2405151110726276049L;

    // Standard data types. Unfortunately HSQLDB doesn't have unsigned data
    // types so we do not set these to unsigned here.
    public static final String TINYINT_UNSIGNED = "TINYINT";
    public static final String SMALLINT_UNSIGNED = "SMALLINT";
    public static final String INT_UNSIGNED = "INT";
    public static final String BIGINT_UNSIGNED = "BIGINT";

    // Shame HSQLDB does not like keys smaller than int.
    public static final String NORMAL_KEY_DEF = INT_UNSIGNED;
    public static final String BIG_KEY_DEF = BIGINT_UNSIGNED;

    public static final String VERSION = "VER";
    public static final String ID = "ID";

    protected static final String FK_PREFIX = "FK_";
    protected static final String ID_SUFFIX = "_ID";
    protected static final String SEP = "_";

    @Transient
    public abstract boolean isPersistent();

    @Transient
    public abstract Object getPrimaryKey();
}
