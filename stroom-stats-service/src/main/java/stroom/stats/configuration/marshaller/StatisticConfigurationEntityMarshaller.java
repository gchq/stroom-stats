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
import stroom.stats.configuration.StatisticConfigurationEntity;
import stroom.stats.configuration.StatisticConfigurationEntityData;

//TODO need an approach to marshalling the entity, i.e. turning the XML in the data col into an object
public class StatisticConfigurationEntityMarshaller extends EntityMarshaller<StatisticConfigurationEntity, StatisticConfigurationEntityData> {
    public StatisticConfigurationEntityMarshaller() {
    }

    @Override
    public StatisticConfigurationEntityData getObject(final StatisticConfigurationEntity entity) {
        return entity.getStatisticDataSourceDataObject();
    }

    @Override
    public void setObject(final StatisticConfigurationEntity entity, final StatisticConfigurationEntityData object) {
        //ensure the field order map is populated and the field list is sorted
        object.reOrderStatisticFields();
        entity.setStatisticDataSourceDataObject(object);

    }

    @Override
    protected String getData(final StatisticConfigurationEntity entity) {
        return entity.getData();
    }

    @Override
    protected void setData(final StatisticConfigurationEntity entity, final String data) {
        entity.setData(data);
    }

    @Override
    protected Class<StatisticConfigurationEntityData> getObjectType() {
        return StatisticConfigurationEntityData.class;
    }

    @Override
    protected String getEntityType() {
        return StatisticConfiguration.ENTITY_TYPE;
    }
}
