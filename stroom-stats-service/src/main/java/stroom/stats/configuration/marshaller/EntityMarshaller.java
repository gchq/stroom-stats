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

import javassist.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stroom.stats.configuration.common.BaseEntity;
import stroom.stats.util.logging.LambdaLogger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;

public abstract class EntityMarshaller<E extends BaseEntity, O> implements Marshaller<E, O> {
    private static final LambdaLogger LOGGER = LambdaLogger.getLogger(EntityMarshaller.class);

    private final JAXBContext jaxbContext;

    public EntityMarshaller() {
        try {
            jaxbContext = JAXBContext.newInstance(getObjectType());
        } catch (final JAXBException e) {
            LOGGER.error("Error getting a new JAXB instance", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public E marshal(final E entity) {
        return marshal(entity, false, false);
    }

    @Override
    public E marshal(final E entity, final boolean external, final boolean ignoreErrors) {
        try {
            final Object object = getObject(entity);

            // Strip out references to empty collections.
            final Object clone = clone(object);

            final String data = XMLMarshallerUtil.marshal(jaxbContext, clone);
            setData(entity, data);
        } catch (final Exception e) {
            LOGGER.debug(() -> String.format("Problem marshaling %s %s", entity.getClass(), entity.getId()), e);
            LOGGER.warn(() -> String.format("Problem marshaling %s %s - %s (enable debug for full trace)", entity.getClass(),
                    entity.getId(), String.valueOf(e)));
        }
        return entity;
    }

    @Override
    public E unmarshal(final E entity) {
        return unmarshal(entity, false, false);
    }

    @Override
    public E unmarshal(final E entity, final boolean external, final boolean ignoreErrors) {
        try {
            final String data = getData(entity);
            final O object = XMLMarshallerUtil.unmarshal(jaxbContext, getObjectType(), data);
            setObject(entity, object);
        } catch (final Exception e) {
            LOGGER.debug("Error un-marshalling entity with message: {}", e.getMessage());
            LOGGER.warn("Error un-marshalling entity with message: {}", e.getMessage());
        }
        return entity;
    }

    private Object clone(final Object obj) {
        return deepClone(obj, 0);
    }

    private Object deepClone(final Object obj, final int depth) {
        Object result = null;
        try {
            if (obj != null) {
                Class<?> clazz = obj.getClass();
                Object clone;

                try {
                    clone = clazz.getConstructor().newInstance();
                } catch (final Exception e) {
                    return obj;
                }

                while (clazz != null) {
                    for (final Field field : clazz.getDeclaredFields()) {
                        if (!(Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers()))) {
                            field.setAccessible(true);
                            try {
                                Object o = field.get(obj);
                                if (o != null) {
                                    if (o instanceof Collection<?>) {
                                        final Collection<?> collection = (Collection<?>) o;
                                        if (collection.size() == 0) {
                                            o = null;
                                        }
                                    } else if (field.getType().isArray()) {
                                        if (Array.getLength(o) == 0) {
                                            o = null;
                                        }
                                    } else {
                                        o = deepClone(o, depth + 1);
                                    }
                                }

                                field.set(clone, o);

                                if (o != null) {
                                    result = clone;
                                }
                            } catch (final Exception e) {
                                LOGGER.error(e.getMessage(), e);
                            }
                        }
                    }
                    clazz = clazz.getSuperclass();
                }

                // If we are at the depth of the initial object then ensure we
                // return a clone of the
                // initial object as we don't want null tobe returned unless
                // null was supplied.
                if (result == null && depth == 0) {
                    result = clone;
                }
            }
        } catch (final Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

        return result;
    }

    protected abstract String getData(E entity);

    protected abstract void setData(E entity, String data);

    protected abstract Class<O> getObjectType();

    protected abstract String getEntityType();
}
