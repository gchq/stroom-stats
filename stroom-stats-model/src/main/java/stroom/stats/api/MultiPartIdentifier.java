/*
 *
 *  * Copyright 2017 Crown Copyright
 *  *
 *  * This library is free software; you can redistribute it and/or modify it under
 *  * the terms of the GNU Lesser General Public License as published by the Free
 *  * Software Foundation; either version 2.1 of the License, or (at your option)
 *  * any later version.
 *  *
 *  * This library is distributed in the hope that it will be useful, but WITHOUT
 *  * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 *  * details.
 *  *
 *  * You should have received a copy of the GNU Lesser General Public License along
 *  * with this library; if not, write to the Free Software Foundation, Inc., 59
 *  * Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *  *
 *  *
 *
 */

package stroom.stats.api;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Class for representing a complex multi-part identifier such as when you
 * have a compound key. If the parts are hierarchical in nature, e.g.
 * BatchNo:IdInBatch then they should be provided to the constructor in the
 * order outer -> inner to ensure correct ordering in the Timeline.
 * Despite the name it will support a single identifier.
 * Currently supported types are: String, Long, Integer, Double, Float, BigDecimal.
 */
public class MultiPartIdentifier {

    private final Object[] values;

    //hold the byte form to save re-computing it all the time
    private  byte[] bValues = null;

//    private MultiPartIdentifier() {
//        //no-arg constructor needed for Kryo de-serialization
//    }

    public MultiPartIdentifier(Object... values) {
        Preconditions.checkNotNull(values);
        if (values.length == 0){
            String valuesStr = values == null ? "NULL" : toHumanReadable();
            throw new IllegalArgumentException(String.format("values %s must contain at least one element", valuesStr));
        }
        this.values = values;
    }

    private byte[] buildBytes(){
        byte[][] byteArrayChunks = new byte[values.length][];

        int i = 0;
        int len = 0;
        for (Object value : values){
            byte[] chunk;
            if (value instanceof String){
                chunk = Bytes.toBytes(String.class.cast(value));
            } else if (value instanceof Integer){
                chunk = Bytes.toBytes(Integer.class.cast(value));
            } else if (value instanceof Long){
                chunk = Bytes.toBytes(Long.class.cast(value));
            } else if (value instanceof Float){
                chunk = Bytes.toBytes(Float.class.cast(value));
            } else if (value instanceof Double){
                chunk = Bytes.toBytes(Double.class.cast(value));
            } else if (value instanceof BigDecimal) {
                chunk = Bytes.toBytes(BigDecimal.class.cast(value));
            } else {
                throw new IllegalArgumentException(String.format("The type %s for value %s is not supported", value.getClass().getName(), value));
            }
            byteArrayChunks[i++] = chunk;
            len += chunk.length;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(len);

        for (byte[] chunk : byteArrayChunks){
            byteBuffer.put(chunk);
        }
        return byteBuffer.array();
    }

    public byte[] getBytes() {
        if (bValues == null){
            bValues = buildBytes();
        }
        return bValues;
    }

    public String toHumanReadable() {
        return StreamSupport.stream(Arrays.spliterator(values),false)
                .map(val -> val.toString())
                .collect(Collectors.joining(":"));
    }

    public Object[] getValue() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MultiPartIdentifier that = (MultiPartIdentifier) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "MultiPartIdentifier{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
