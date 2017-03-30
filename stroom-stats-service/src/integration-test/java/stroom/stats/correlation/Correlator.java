package stroom.stats.correlation;

import com.google.common.collect.ImmutableList;
import stroom.query.api.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Correlator {

    private Map<SetName, ImmutableList<Row>> sets = new HashMap<>();

    public Correlator addSet(SetName setName, List<Row> set){
        sets.put(setName, ImmutableList.copyOf(set));
        return this;
    }

    public List<Row> complement(SetName setName){
        ImmutableList<Row> set = sets.get(setName);
        List<Row> complementOfSet = sets.entrySet().stream()
                .filter(entry -> entry.getKey() != setName) // Get rid of our complement set
                .flatMap(entry -> entry.getValue().stream()) // We don't care about sets now, just about rows
                .filter(row -> !set.contains(row)) // We only want stuff not in our main set
                .collect(Collectors.toList());
        return complementOfSet;
    }

    public List<Row> intersection(SetName... setNames) {
        List<ImmutableList<Row>> setsForIntersection = new ArrayList<>();
        Stream.of(setNames).forEach(setName -> setsForIntersection.add(sets.get(setName)));

        if(setsForIntersection.size() >= 2){
//            intersection(se)
        }
        return null; //TODO
    }

    private Set<Row> intersection(Set<Row> intersectionAccumulator, List<Row>... remainingSets){
        if(remainingSets.length >= 2) {
            return intersectionAccumulator.stream().filter(remainingSets[0]::contains).collect(Collectors.toSet());
        }
        else{
            return intersectionAccumulator;
        }
        //TODO recurse
    }

    public enum SetName {
        A, B, C, D, E;
    }
}
