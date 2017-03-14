

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

package stroom.stats.configuration;

import stroom.stats.configuration.common.HasDisplayValue;
import stroom.stats.configuration.common.SharedObject;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "customRollUpMask")
public class CustomRollUpMaskEntityObject extends AbstractCustomRollUpMask implements HasDisplayValue, SharedObject {
    private static final long serialVersionUID = 5978256629347842695L;

    /**
     * Holds a list of the positions of tags that are rolled up, zero based. The
     * position number is based on the alphanumeric sorted list of tag/field
     * names in the {@link StatisticConfigurationEntity}. Would use a SortedSet but that
     * is not supported by GWT. Must ensure the contents of this are sorted so
     * that when contains is called on lists of these objects it works
     * correctly.
     */
    //No-arg constructore needed for JAXB unmarshalling
    @SuppressWarnings("unused")
    public CustomRollUpMaskEntityObject() {
    }

    public CustomRollUpMaskEntityObject(final List<Integer> rolledUpTagPositions) {
        super(rolledUpTagPositions);
    }

    @XmlElement(name = "rolledUpTagPosition")
    @Override
    public List<Integer> getRolledUpTagPositions() {
        return super.getRolledUpTagPositions();
    }

    @Override
    public void setRolledUpTagPositions(final List<Integer> rolledUpTagPositions) {
        super.setRolledUpTagPositions(rolledUpTagPositions);
    }


    @Override
    public String getDisplayValue() {
        return null;
    }

    @Override
    public CustomRollUpMask deepCopy() {
        return new CustomRollUpMaskEntityObject(super.deepCloneTagPositions());
    }

}
