

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

package stroom.stats.hbase.uid;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class defines the codes used to qualify each row key in the UID table
 */
public enum UniqueIdType {
    // Marks the rowkey as being a UID byte array
    UID("U"),
    // Marks the rowkey as being the string name that corresponds to a UID
    NAME("N"),
    // The row that holds the max ID currently held, used for generating new UID
    // byte arrays
    MAX_ID("M");

    final String typeCode;
    final byte[] typeCodeBytes;

    private static Map<String, UniqueIdType> bytesToEnumMap = new HashMap<>();

    static {
        for (final UniqueIdType uniqueIdType : UniqueIdType.values()) {
            bytesToEnumMap.put(uniqueIdType.getColumnQualifier(), uniqueIdType);
        }
    }

    UniqueIdType(final String typeCode) {
        this.typeCode = typeCode;
        this.typeCodeBytes = Bytes.toBytes(typeCode);
    }

    public byte[] asByteArray() {
        return typeCodeBytes;
    }

    public String getColumnQualifier() {
        return typeCode;
    }

    public static UniqueIdType fromTypeCode(final String typeCode) {
        return bytesToEnumMap.get(typeCode);
    }
}
