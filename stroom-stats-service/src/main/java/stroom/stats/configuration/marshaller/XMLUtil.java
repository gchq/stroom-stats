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
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import stroom.util.xml.SAXParserFactoryFactory;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public final class XMLUtil {
    public static final Logger LOGGER = LoggerFactory.getLogger(XMLUtil.class);

    public static final SAXParserFactory PARSER_FACTORY;

    private static final String XML = "xml";
    private static final String UTF_8 = "UTF-8";
    private static final String NO = "no";
    private static final String YES = "yes";
    private static final String VERSION = "1.1";

    static {
        PARSER_FACTORY = SAXParserFactoryFactory.newInstance();
        PARSER_FACTORY.setNamespaceAware(true);
    }

    private XMLUtil() {
        // Hidden constructor.
    }

    /**
     * Convert a java type into a xml name E.g. XMLType = "xmlType", String =
     * "string", StringBIG = "StringBIG"
     */
    public static final String toXMLName(final String name) {
        final StringBuilder builder = new StringBuilder();
        boolean firstWord = true;
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);

            if (i == 0) {
                builder.append(Character.toLowerCase(c));
            } else {
                if (firstWord) {
                    if (i + 2 < name.length() && Character.isLowerCase(name.charAt(i + 2))) {
                        firstWord = false;
                    }
                    builder.append(Character.toLowerCase(c));
                } else {
                    builder.append(c);
                }
            }
        }

        return builder.toString();
    }

    public static String prettyPrintXML(final String xml) {
        final Reader reader = new StringReader(xml);
        final Writer writer = new StringWriter(1000);

        prettyPrintXML(reader, writer);

        return writer.toString();
    }

    public static void prettyPrintXML(final InputStream inputStream, final OutputStream outputStream) {
        final Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        final Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);

        prettyPrintXML(reader, writer);
    }

    private static void prettyPrintXML(final Reader reader, final Writer writer) {
        try {
            final TransformerHandler handler = createTransformerHandler(new FatalErrorListener(), true);
            handler.setResult(new StreamResult(writer));

            final SAXParser parser = PARSER_FACTORY.newSAXParser();
            final XMLReader xmlReader = parser.getXMLReader();
            xmlReader.setErrorHandler(new FatalErrorHandler());
            xmlReader.setContentHandler(handler);
            xmlReader.parse(new InputSource(reader));

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TransformerHandler createTransformerHandler(final boolean indentOutput)
            throws TransformerConfigurationException {
        return createTransformerHandler(null, indentOutput);
    }

    public static TransformerHandler createTransformerHandler(final ErrorListener errorListener,
                                                              final boolean indentOutput) throws TransformerConfigurationException {
        final SAXTransformerFactory stf = (SAXTransformerFactory) TransformerFactoryFactory.newInstance();
        if (errorListener != null) {
            stf.setErrorListener(errorListener);
        }

        final TransformerHandler th = stf.newTransformerHandler();
        final Transformer transformer = th.getTransformer();
        setCommonOutputProperties(transformer, indentOutput);

        if (errorListener != null) {
            transformer.setErrorListener(errorListener);
        }

        return th;
    }

    public static void setCommonOutputProperties(final Transformer transformer, final boolean indentOutput)
            throws TransformerConfigurationException {
        transformer.setOutputProperty(OutputKeys.METHOD, XML);
        transformer.setOutputProperty(OutputKeys.ENCODING, UTF_8);
        transformer.setOutputProperty(OutputKeys.VERSION, VERSION);
        if (indentOutput) {
            transformer.setOutputProperty(OutputKeys.INDENT, YES);
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "3");
        } else {
            transformer.setOutputProperty(OutputKeys.INDENT, NO);
        }
    }

}
