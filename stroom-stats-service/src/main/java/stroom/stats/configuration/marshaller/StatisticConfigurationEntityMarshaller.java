/*
 * Copyright 2017 Crown Copyright
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to the Free Software Foundation, Inc., 59
 * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 *
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
