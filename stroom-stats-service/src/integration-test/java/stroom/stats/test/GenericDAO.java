package stroom.stats.test;

import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class GenericDAO<T> extends AbstractDAO<T> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public GenericDAO(final SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    @Override
    public T persist(final T entity) throws HibernateException {
        return super.persist(entity);
    }

    @Override
    public T get(final Serializable id) {
        return super.get(id);
    }

    public Optional<T> getByName(final String name) {
        List entities = super.criteria()
                .add(Restrictions.eq("name", name))
                .list();
        if (entities == null || entities.size() == 0) {
            return Optional.empty();
        } else if (entities.size() > 1) {
            throw new RuntimeException(String.format("Name %s was not unique, %s entities found", name, entities.size()));
        } else {
            return Optional.of((T) entities.get(0));
        }
    }
}
