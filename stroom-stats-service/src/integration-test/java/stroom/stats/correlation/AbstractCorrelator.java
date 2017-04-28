package stroom.stats.correlation;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class AbstractCorrelator<T, E> implements Correlator<T, E> {

    private Map<String, ImmutableSet<E>> sets = new HashMap<>();

    private final Function<T, Collection<E>> collectionExtractor;
    private final Function<Collection<E>, T> collectionWrapper;

    public AbstractCorrelator(final Function<T, Collection<E>> collectionExtractor,
                              final Function<Collection<E>, T> collectionWrapper) {
        this.collectionExtractor = collectionExtractor;
        this.collectionWrapper = collectionWrapper;
    }

    @Override
    public Correlator<T, E> addSet(String setName, T container){
        sets.put(setName, ImmutableSet.copyOf(collectionExtractor.apply(container)));
        return this;
    }

    @Override
    public T complement(String setName) {
        ImmutableSet<E> set = sets.get(setName);
        List<E> complementOfSet = sets.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(setName)) // Get rid of our complement set
                .flatMap(entry -> entry.getValue().stream()) // We don't care about sets now, just about rows
                .filter(row -> !set.contains(row)) // We only want stuff not in our main set
                .collect(Collectors.toList());
        return collectionWrapper.apply(complementOfSet);
    }

    @Override
    public T intersection(String... setNames) {
        assertThat(setNames.length).isGreaterThanOrEqualTo(2);
        List<ImmutableSet<E>> setsForIntersection = new ArrayList<>();
        Stream.of(setNames)
                .forEach(setName -> setsForIntersection.add(sets.get(setName)));

        Set<E> firstIntersection = setsForIntersection.get(0).stream()
                .filter(setsForIntersection.get(1)::contains)
                .collect(Collectors.toSet());

        if(setsForIntersection.size() >= 3 ) {
            List<ImmutableSet<E>> remainingSets = setsForIntersection.subList(2, setsForIntersection.size());
            return collectionWrapper.apply(intersection(firstIntersection, remainingSets));
        }
        else{
            return collectionWrapper.apply(firstIntersection);
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

    Set<E> getSet(final String name) {
        return sets.get(name);
    }

}
