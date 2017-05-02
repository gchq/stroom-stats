package stroom.stats.correlation;

/**
 * Implementations will correlate objects of type T containing a collection of elements of
 * type E. Performs set operations on two or more instances of T
 * @param <T> The object containing the data to be correlated
 * @param <E> The type of the elements to be correlated
 */
public interface Correlator<T, E> {

    Correlator<T, E> addSet(String setName, T container);

    T complement(String setName);

    T intersection(String... setNames);

    boolean isEmpty();

}
