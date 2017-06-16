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

package stroom.stats.test;

import com.google.common.base.Preconditions;
import stroom.stats.api.StatisticType;
import stroom.stats.configuration.CustomRollUpMask;
import stroom.stats.configuration.CustomRollUpMaskEntityObject;
import stroom.stats.configuration.StatisticField;
import stroom.stats.configuration.StatisticRollUpType;
import stroom.stats.configuration.StroomStatsStoreEntity;
import stroom.stats.configuration.StroomStatsStoreEntityData;
import stroom.stats.configuration.common.Folder;
import stroom.stats.shared.EventStoreTimeIntervalEnum;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

public class StroomStatsStoreEntityBuilder {

    private final StroomStatsStoreEntity stroomStatsStoreEntity;
    private final StroomStatsStoreEntityData stroomStatsStoreEntityData;

    public StroomStatsStoreEntityBuilder(final String statName,
                                         final StatisticType statisticType,
                                         final EventStoreTimeIntervalEnum interval,
                                         final StatisticRollUpType statisticRollUpType) {

        Preconditions.checkNotNull(statName);
        Preconditions.checkNotNull(statisticType);
        Preconditions.checkNotNull(statisticRollUpType);

        stroomStatsStoreEntity = new StroomStatsStoreEntity();
        stroomStatsStoreEntity.setName(statName);
        stroomStatsStoreEntity.setStatisticType(statisticType);
        stroomStatsStoreEntity.setPrecision(interval);
        stroomStatsStoreEntity.setRollUpType(statisticRollUpType);
        stroomStatsStoreEntity.setUuid(UUID.randomUUID().toString());

        //Use the same folder for all entities
        Folder folder = Folder.create(null, "RootFolder");
        folder.setUuid(UUID.randomUUID().toString());
        stroomStatsStoreEntity.setFolder(folder);

        stroomStatsStoreEntityData = new StroomStatsStoreEntityData();
        stroomStatsStoreEntity.setDataObject(stroomStatsStoreEntityData);
    }

    public StroomStatsStoreEntityBuilder addFields(final String... fields) {

        stroomStatsStoreEntityData.setStatisticFields(
                Arrays.stream(fields)
                        .map(StatisticField::new)
                        .collect(Collectors.toList()));
        return this;
    }

    public StroomStatsStoreEntityBuilder addCustomMasks(final CustomRollUpMask... customRollUpMasks) {

        stroomStatsStoreEntityData.setCustomRollUpMasks(
                Arrays.stream(customRollUpMasks)
                        .map(mask -> new CustomRollUpMaskEntityObject(mask.getRolledUpTagPositions()))
                        .collect(Collectors.toSet()));
        return this;
    }

    public StroomStatsStoreEntity build() {
        return stroomStatsStoreEntity;
    }
}
