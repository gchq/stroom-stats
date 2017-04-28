package stroom.stats.correlation;

public interface Correlator<T, E> {

    Correlator<T, E> addSet(String setName, T container);

    T complement(String setName);

    T intersection(String... setNames);

    boolean isEmpty();

}
