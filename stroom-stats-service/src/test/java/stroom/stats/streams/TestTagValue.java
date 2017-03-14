/*
 *
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
 */

package stroom.stats.streams;

import org.junit.Test;
import stroom.stats.hbase.uid.UID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTagValue {

    UID tag1 = UID.from(new byte[] {0,0,0,1});
    UID value1 = UID.from(new byte[] {0,0,0,2});
    TagValue tagValue1 = new TagValue(tag1, value1);
    UID tag2 = UID.from(new byte[] {0,0,1,1});
    UID value2 = UID.from(new byte[] {0,0,1,2});
    TagValue tagValue2 = new TagValue(tag2, value2);

    @Test
    public void getBytes() throws Exception {

        assertThat(tagValue1.getBytes()).isEqualTo(new byte[] {0,0,0,1,0,0,0,2});
    }

    @Test
    public void getTagAndValue() throws Exception {

        UID tag = tagValue1.getTag();
        UID value = tagValue1.getValue();

        assertThat(tag.getUidBytes()).isEqualTo(new byte[] {0,0,0,1});
        assertThat(value.getUidBytes()).isEqualTo(new byte[] {0,0,0,2});

        assertThat(tagValue1.getTag().getUidBytes()).isEqualTo(new byte[] {0,0,0,1});
        assertThat(tagValue1.getValue().getUidBytes()).isEqualTo(new byte[] {0,0,0,2});
    }


    @Test
    public void cloneAndRollUp() throws Exception {

        UID rolledUpValue = UID.from(new byte[] {0,0,0,3});

        TagValue rolledUpTagValue = tagValue1.cloneAndRollUp(rolledUpValue);

        //original tagvalue is unchanged
        assertThat(tagValue1.getTag().getUidBytes()).isEqualTo(new byte[] {0,0,0,1});
        assertThat(tagValue1.getValue().getUidBytes()).isEqualTo(new byte[] {0,0,0,2});

        //rolled up tagvalue has a new value
        assertThat(rolledUpTagValue.getTag().getUidBytes()).isEqualTo(new byte[] {0,0,0,1});
        assertThat(rolledUpTagValue.getValue().getUidBytes()).isEqualTo(new byte[] {0,0,0,3});

    }

    @Test
    public void compareTo_equal() {
        UID tag3 = UID.from(new byte[] {0,0,0,1});
        UID value3 = UID.from(new byte[] {0,0,0,2});
        TagValue tagValue3 = new TagValue(tag3, value3);


        assertThat(tagValue1).isEqualTo(tagValue3);
    }

//    @Test
//    public void compareTo_tag2bigger() {
//        byte[] bTag1 = {0,0,0,1};
//        byte[] bValue1 = {0,0,0,2};
//        TagValue tagValue1 = new TagValue(bTag1, bValue1);
//
//        byte[] bTag2 = {0,0,1,1};
//        byte[] bValue2 = {0,0,0,2};
//        TagValue tagValue2 = new TagValue(bTag2, bValue2);
//
//        assertThat(tagValue2).isGreaterThan(tagValue1);
//    }
//
//    @Test
//    public void compareTo_tag2lessThan() {
//        byte[] bTag1 = {0,0,1,1};
//        byte[] bValue1 = {0,0,0,2};
//        TagValue tagValue1 = new TagValue(bTag1, bValue1);
//
//        byte[] bTag2 = {0,0,0,1};
//        byte[] bValue2 = {0,0,0,2};
//        TagValue tagValue2 = new TagValue(bTag2, bValue2);
//
//        assertThat(tagValue2).isLessThan(tagValue1);
//    }
//
//    @Test
//    public void compareTo_value2bigger() {
//        byte[] bTag1 = {0,0,0,1};
//        byte[] bValue1 = {0,0,0,2};
//        TagValue tagValue1 = new TagValue(bTag1, bValue1);
//
//        byte[] bTag2 = {0,0,0,1};
//        byte[] bValue2 = {0,0,1,1};
//        TagValue tagValue2 = new TagValue(bTag2, bValue2);
//
//        assertThat(tagValue2).isGreaterThan(tagValue1);
//    }
//
//    @Test
//    public void compareTo_value2lessThan() {
//        byte[] bTag1 = {0,0,0,1};
//        byte[] bValue1 = {0,0,1,2};
//        TagValue tagValue1 = new TagValue(bTag1, bValue1);
//
//        byte[] bTag2 = {0,0,0,1};
//        byte[] bValue2 = {0,0,0,2};
//        TagValue tagValue2 = new TagValue(bTag2, bValue2);
//
//        assertThat(tagValue2).isLessThan(tagValue1);
//    }

    @Test
    public void compareTo() throws Exception {

        assertThat(doCompare(tag1, value1, tag1, value1)).isEqualTo(0);

        assertThat(doCompare(tag1, value1, tag2, value1)).isLessThan(0);
        assertThat(doCompare(tag2, value1, tag1, value1)).isGreaterThan(0);

        assertThat(doCompare(tag1, value1, tag1, value2)).isLessThan(0);
        assertThat(doCompare(tag1, value2, tag1, value1)).isGreaterThan(0);
    }

    private int doCompare(UID t1, UID v1, UID t2, UID v2) {
        return new TagValue(t1, v1).compareTo(new TagValue(t2, v2));
    }
}