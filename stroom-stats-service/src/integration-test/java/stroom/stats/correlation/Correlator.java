package stroom.stats.correlation;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class Correlator<T> {

    private Map<String, ImmutableSet<T>> sets = new HashMap<>();

    public Correlator addSet(String setName, Collection<T> set){
        sets.put(setName, ImmutableSet.copyOf(set));
        return this;
    }

    public List<T> complement(String setName) {
        ImmutableSet<T> set = sets.get(setName);
        List<T> complementOfSet = sets.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(setName)) // Get rid of our complement set
                .flatMap(entry -> entry.getValue().stream()) // We don't care about sets now, just about rows
                .filter(row -> !set.contains(row)) // We only want stuff not in our main set
                .collect(Collectors.toList());
        return complementOfSet;
    }

    public Set<T> intersection(String... setNames) {
        assertThat(setNames.length).isGreaterThanOrEqualTo(2);
        List<ImmutableSet<T>> setsForIntersection = new ArrayList<>();
        Stream.of(setNames).forEach(setName -> setsForIntersection.add(sets.get(setName)));

        Set<T> firstIntersection = setsForIntersection.get(0).stream()
                    .filter(setsForIntersection.get(1)::contains).collect(Collectors.toSet());

        if(setsForIntersection.size() >= 3 ) {
            List<ImmutableSet<T>> remainingSets = setsForIntersection.subList(2, setsForIntersection.size());
            return intersection(firstIntersection, remainingSets);
        }
        else{
            return firstIntersection;
        }
    }


    private Set<T> intersection(Set<T> intersectionAccumulator, List<ImmutableSet<T>> remainingSets){
        if(remainingSets.size() > 0) {
            Set<T> newIntersection = intersectionAccumulator.stream().filter(remainingSets.get(0)::contains).collect(Collectors.toSet());
            remainingSets.remove(0);
            return intersection(newIntersection, remainingSets);
        }
        return intersectionAccumulator;
    }
}
