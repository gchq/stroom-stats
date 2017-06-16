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

package stroom.stats.configuration.marshaller;


import stroom.stats.configuration.StatisticConfiguration;
import stroom.stats.configuration.StroomStatsStoreEntity;
import stroom.stats.configuration.StroomStatsStoreEntityData;

//TODO need an approach to marshalling the entity, i.e. turning the XML in the data col into an object
public class StroomStatsStoreEntityMarshaller
        extends EntityMarshaller<StroomStatsStoreEntity, StroomStatsStoreEntityData> {
    public StroomStatsStoreEntityMarshaller() {
    }

    @Override
    public StroomStatsStoreEntityData getObject(final StroomStatsStoreEntity entity) {
        return entity.getDataObject();
    }

    @Override
    public void setObject(final StroomStatsStoreEntity entity, final StroomStatsStoreEntityData object) {
        //ensure the field order map is populated and the field list is sorted
        object.reOrderStatisticFields();
        entity.setDataObject(object);

    }

    @Override
    protected String getData(final StroomStatsStoreEntity entity) {
        return entity.getData();
    }

    @Override
    protected void setData(final StroomStatsStoreEntity entity, final String data) {
        entity.setData(data);
    }

    @Override
    protected Class<StroomStatsStoreEntityData> getObjectType() {
        return StroomStatsStoreEntityData.class;
    }

    @Override
    protected String getEntityType() {
        return StatisticConfiguration.ENTITY_TYPE;
    }
}
