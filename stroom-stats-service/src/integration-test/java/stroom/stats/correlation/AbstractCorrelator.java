package stroom.stats.correlation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class AbstractCorrelator<T, E> implements Correlator<T, E> {

    private Map<String, ImmutableSet<E>> sets = new HashMap<>();

    private final CollectionExtractor<T, E> collectionExtractor;
    private final CollectionWrapper<T, E> collectionWrapper;

    public interface CollectionExtractor<T, E> {
        Collection<E> extract (final T wrapper);
    }

    public interface CollectionWrapper<T, E> {
        T wrap (final Collection<E> collection);
    }

    /**
     * @param collectionExtractor Function to extract
     * @param collectionWrapper
     */
    public AbstractCorrelator(final CollectionExtractor<T, E> collectionExtractor,
                              final CollectionWrapper<T, E> collectionWrapper) {
        this.collectionExtractor = collectionExtractor;
        this.collectionWrapper = collectionWrapper;
    }

    @Override
    public Correlator<T, E> addSet(String setName, T container){
        Preconditions.checkNotNull(setName);
        Preconditions.checkArgument(!setName.isEmpty(), "setName cannot be empty");
        Preconditions.checkNotNull(container);
        sets.put(setName, ImmutableSet.copyOf(collectionExtractor.extract(container)));
        return this;
    }

    @Override
    public T complement(String setName) {
        if (isEmpty()) {
            throw new RuntimeException("Correlator is empty");
        }

        ImmutableSet<E> set = getSet(setName);

        List<E> complementOfSet = sets.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(setName)) // Get rid of our complement set
                .flatMap(entry -> entry.getValue().stream()) // We don't care about sets now, just about rows
                .filter(row -> !set.contains(row)) // We only want stuff not in our main set
                .collect(Collectors.toList());
        return collectionWrapper.wrap(complementOfSet);
    }

    @Override
    public T intersection(String... setNames) {
        assertThat(setNames.length).isGreaterThanOrEqualTo(2);
        List<ImmutableSet<E>> setsForIntersection = new ArrayList<>();
        Stream.of(setNames)
                .forEach(setName ->
                        setsForIntersection.add(getSet(setName)));

        Set<E> firstIntersection = setsForIntersection.get(0).stream()
                .filter(setsForIntersection.get(1)::contains)
                .collect(Collectors.toSet());

        if(setsForIntersection.size() >= 3 ) {
            List<ImmutableSet<E>> remainingSets = setsForIntersection.subList(2, setsForIntersection.size());
            return collectionWrapper.wrap(intersection(firstIntersection, remainingSets));
        }
        else{
            return collectionWrapper.wrap(firstIntersection);
        }
    }

    @Override
    public boolean isEmpty() {
        return sets.isEmpty();
    }

    private Set<E> intersection(Set<E> intersectionAccumulator, List<ImmutableSet<E>> remainingSets){
        if(remainingSets.size() > 0) {
            Set<E> newIntersection = intersectionAccumulator.stream()
                    .filter(remainingSets.get(0)::contains)
                    .collect(Collectors.toSet());
            remainingSets.remove(0);
            return intersection(newIntersection, remainingSets);
        }
        return intersectionAccumulator;
    }

    ImmutableSet<E> getSet(final String name) {
        ImmutableSet<E> set = sets.get(name);
        if (set == null) {
            throw new RuntimeException(String.format("Set with name %s does not exist in the correlator", name));
        }
        return set;
    }
}
