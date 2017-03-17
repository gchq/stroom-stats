

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

package stroom.stats.hbase.table.filter;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import stroom.stats.hbase.structure.RowKey;

/**
 * This filter is designed to be deployed onto the HBase server itself so the
 * filtering is done server-side. In order to do this the filter must be
 * serialisable to a byte[] and back again. Here we have used jaxb to serialise
 * the filter and the associated filter tree object. This class and any
 * non-standard classes must be packaged up in a jar by the
 * stroom.stats.hbase-filer maven module.
 *
 * If HBase is running on the cluster then the filter jar file should be placed
 * in /hbase/lib on the Hadoop file system i.e. sudo -u hbase hdfs dfs
 * -copyFromLocal /
 * <pathToJarFileOnLocalFileSystem>/stroom-satistics-hbase-filter-X.X.X-SNAPSHOT-
 * fat-jar.jar /hbase/lib/
 */
public class StatisticsTagValueFilter extends AbstractTagValueFilter {

    private static final int START_OF_TAG_VALUE_PAIRS_POSITION = 0 + RowKey.UID_AND_BIT_MASK_AND_TIME_LENGTH;

    public StatisticsTagValueFilter(final TagValueFilterTree tagValueFilterTree) {
        super(tagValueFilterTree);
    }

    @Override
    TagValueSection findTagValueSectionOfRowKey(final byte[] buffer, final int offset, final int length) {
        final int from = offset + START_OF_TAG_VALUE_PAIRS_POSITION;
        final int to = offset + length;

        return new TagValueSection(from, to);
    }

    /**
     * Rebuild an instance of this class from a byte array. This will be called by the Hbase server
     * to de-serialize the class from bytes
     *
     * @throws DeserializationException
     */
    @SuppressWarnings("unused")
    public static StatisticsTagValueFilter parseFrom(final byte[] value) throws DeserializationException {
        final TagValueFilterTree tagValueFilterTree = TagValueFilterTreeSerialiser.instance().deserialize(value);

        return new StatisticsTagValueFilter(tagValueFilterTree);
    }
}
