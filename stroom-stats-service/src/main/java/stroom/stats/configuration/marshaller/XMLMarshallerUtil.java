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


import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public final class XMLMarshallerUtil {
    private XMLMarshallerUtil() {
        // Utility class.
    }

    public static <T> String marshal(final JAXBContext context, final T obj, final XmlAdapter<?, ?>... adapters) {
        if (obj == null) {
            return null;
        }

        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final Marshaller marshaller = context.createMarshaller();

            if (adapters != null) {
                for (final XmlAdapter<?, ?> adapter : adapters) {
                    marshaller.setAdapter(adapter);
                }
            }

            final TransformerHandler transformerHandler = XMLUtil.createTransformerHandler(true);
            transformerHandler.setResult(new StreamResult(out));
            marshaller.marshal(obj, transformerHandler);

            return out.toString(String.valueOf(StandardCharsets.UTF_8));
        } catch (final Throwable t) {
            throw new RuntimeException(t.getMessage(), t);
        }
    }

    public static <T> T unmarshal(final JAXBContext context, final Class<T> clazz, final String data,
            final XmlAdapter<?, ?>... adapters) {
        if (data != null) {
            final String trimmed = data.trim();
            if (trimmed.length() > 0) {
                try {
                    final Unmarshaller unmarshaller = context.createUnmarshaller();

                    if (adapters != null) {
                        for (final XmlAdapter<?, ?> adapter : adapters) {
                            unmarshaller.setAdapter(adapter);
                        }
                    }

                    final JAXBElement<T> jaxbElement = unmarshaller.unmarshal(
                            new StreamSource(new ByteArrayInputStream(trimmed.getBytes(StandardCharsets.UTF_8))),
                            clazz);
                    return jaxbElement.getValue();
                } catch (final JAXBException e) {
                    throw new RuntimeException("Invalid XML " + trimmed, e);
                }
            }
        }
        return null;
    }
}
