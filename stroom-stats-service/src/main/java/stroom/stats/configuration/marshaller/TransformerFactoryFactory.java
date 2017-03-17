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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.TransformerFactory;

public final class TransformerFactoryFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformerFactoryFactory.class);

    private static final String SAXON_TRANSFORMER_FACTORY = "net.sf.saxon.TransformerFactoryImpl";
    private static final String IMP_USED = "The transformer factory implementation being used is: ";
    private static final String END = "\".";
    private static final String SYSPROP_SET_TO = "System property \"javax.xml.transform.TransformerFactory\" set to \"";
    private static final String SYSPROP_NOT_SET = "System property \"javax.xml.transform.TransformerFactory\" not set.";
    private static final String SYSPROP_TRANSFORMER_FACTORY = "javax.xml.transform.TransformerFactory";

    static {
        try {
            final String factoryName = System.getProperty(SYSPROP_TRANSFORMER_FACTORY);
            if (factoryName == null) {
                LOGGER.info(SYSPROP_NOT_SET);

                System.setProperty(SYSPROP_TRANSFORMER_FACTORY, SAXON_TRANSFORMER_FACTORY);
            } else {
                final StringBuilder sb = new StringBuilder();
                sb.append(SYSPROP_SET_TO);
                sb.append(factoryName);
                sb.append(END);
                LOGGER.info(sb.toString());
            }

            final TransformerFactory factory = TransformerFactory.newInstance();
            final StringBuilder sb = new StringBuilder();
            sb.append(IMP_USED);
            sb.append(factory.getClass().getName());
            LOGGER.info(sb.toString());
        } catch (Exception ex) {
            LOGGER.error("Error getting new TransformerFactory instance", ex);
        }
    }

    private TransformerFactoryFactory() {
        // Utility class.
    }

    public static TransformerFactory newInstance() {
        return TransformerFactory.newInstance();
    }
}
