package stroom.stats.correlation;

import com.google.common.collect.ImmutableSet;
import stroom.query.api.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class Correlator {

    private Map<String, ImmutableSet<Row>> sets = new HashMap<>();

    public Correlator addSet(String setName, Set<Row> set){
        sets.put(setName, ImmutableSet.copyOf(set));
        return this;
    }

    public List<Row> complement(String setName){
        ImmutableSet<Row> set = sets.get(setName);
        List<Row> complementOfSet = sets.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(setName)) // Get rid of our complement set
                .flatMap(entry -> entry.getValue().stream()) // We don't care about sets now, just about rows
                .filter(row -> !set.contains(row)) // We only want stuff not in our main set
                .collect(Collectors.toList());
        return complementOfSet;
    }

    public Set<Row> intersection(String... setNames) {
        assertThat(setNames.length).isGreaterThanOrEqualTo(2);
        List<ImmutableSet<Row>> setsForIntersection = new ArrayList<>();
        Stream.of(setNames).forEach(setName -> setsForIntersection.add(sets.get(setName)));

        Set<Row> firstIntersection = setsForIntersection.get(0).stream()
                    .filter(setsForIntersection.get(1)::contains).collect(Collectors.toSet());

        if(setsForIntersection.size() >= 3 ) {
            List<ImmutableSet<Row>> remainingSets = setsForIntersection.subList(2, setsForIntersection.size());
            return intersection(firstIntersection, remainingSets);
        }
        else{
            return firstIntersection;
        }
    }


    private Set<Row> intersection(Set<Row> intersectionAccumulator, List<ImmutableSet<Row>> remainingSets){
        if(remainingSets.size() > 0) {
            Set<Row> newIntersection = intersectionAccumulator.stream().filter(remainingSets.get(0)::contains).collect(Collectors.toSet());
            remainingSets.remove(0);
            return intersection(newIntersection, remainingSets);
        }
        return intersectionAccumulator;
    }
}
