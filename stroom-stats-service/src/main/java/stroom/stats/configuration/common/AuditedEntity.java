

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@MappedSuperclass
public abstract class AuditedEntity extends BaseEntitySmall {
    private static final long serialVersionUID = -2887373467654809485L;

    public static final String CREATE_TIME = "CRT_MS";
    public static final String CREATE_USER = "CRT_USER";
    public static final String UPDATE_TIME = "UPD_MS";
    public static final String UPDATE_USER = "UPD_USER";

    /**
     * The gets set depending on if we are running in the server side with a
     * spring context.
     */
    private static CurrentUserResolver currentUserResolver;

    public static void setCurrentUserResolver(final CurrentUserResolver acurrentUserResolver) {
        currentUserResolver = acurrentUserResolver;
    }

    private Long createTime;
    private Long updateTime;
    private String createUser;
    private String updateUser;

    public static final Set<String> AUDIT_FIELDS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("createTime", "updateTime", "createUser", "updateUser")));

    @Override
    public void clearPersistence() {
        super.clearPersistence();
    }

    @Column(name = CREATE_TIME, columnDefinition = BIGINT_UNSIGNED)
    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(final Long createTime) {
        this.createTime = createTime;
    }

    @Column(name = UPDATE_TIME, columnDefinition = BIGINT_UNSIGNED)
    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(final Long updateTime) {
        this.updateTime = updateTime;
    }

    @Column(name = CREATE_USER)
    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(final String createUser) {
        this.createUser = createUser;
    }

    @Column(name = UPDATE_USER)
    public String getUpdateUser() {
        return updateUser;
    }

    public void setUpdateUser(final String updateUser) {
        this.updateUser = updateUser;
    }

    /**
     * @return the user who is logged in or NULL.
     */
    private static String getCurrentUser() {
        if (currentUserResolver != null) {
            return currentUserResolver.getCurrentUser();
        }
        return null;
    }

    /**
     * JPA hook.
     */
    @Override
    public void prePersist() {
        final long now = System.currentTimeMillis();
        setCreateTime(now);
        setCreateUser(getCurrentUser());
        setUpdateTime(now);
        setUpdateUser(getCurrentUser());
    }

    /**
     * JPA hook.
     */
    @Override
    public void preUpdate() {
        final long now = System.currentTimeMillis();
        setUpdateTime(now);
        setUpdateUser(getCurrentUser());
    }
}
