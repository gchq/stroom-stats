package stroom.stats;

import stroom.mapreduce.v2.UnsafePairQueue;
import stroom.query.common.v2.Coprocessor;
import stroom.query.common.v2.CoprocessorSettingsMap;
import stroom.query.common.v2.Data;
import stroom.query.common.v2.Item;
import stroom.query.common.v2.Items;
import stroom.query.common.v2.ItemsArrayList;
import stroom.query.common.v2.Key;
import stroom.query.common.v2.Payload;
import stroom.query.common.v2.Store;
import stroom.query.common.v2.StoreSize;
import stroom.query.common.v2.TablePayload;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatisticsStore implements Store {

    private CoprocessorSettingsMap coprocessorSettingsMap;
    private Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap;
    private Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap;

    @Override
    public void destroy() {

    }

    @Override
    public boolean isComplete() {
        return true;
    }

    @Override
    public Data getData(String componentId) {
        final CoprocessorSettingsMap.CoprocessorKey coprocessorKey = coprocessorSettingsMap.getCoprocessorKey(componentId);
        if (coprocessorKey == null) {
            return null;
        }

        Coprocessor coprocessor = coprocessorMap.get(coprocessorKey);
        Payload payload = payloadMap.get(coprocessorKey);
        TablePayload tablePayload = (TablePayload)payload;
        UnsafePairQueue<Key, Item> queue = tablePayload.getQueue();

        Map<Key, Items<Item>> childMap = new HashMap<>();
        // We should now have a reduction in the reducedQueue.
        queue.forEach(pair -> {
            final Item item = pair.getValue();

            if (item.getKey() != null) {
                childMap.computeIfAbsent(item.getKey().getParent(), k -> new ItemsArrayList<>()).add(item);
            } else {
                childMap.computeIfAbsent(null, k -> new ItemsArrayList<>()).add(item);
            }
        });


        return new Data(childMap, queue.size(), queue.size());
    }

    @Override
    public List<String> getErrors() {
        return null;
    }

    @Override
    public List<String> getHighlights() {
        return null;
    }

    @Override
    public List<Integer> getDefaultMaxResultsSizes() {
        return null;
    }

    @Override
    public StoreSize getStoreSize() {
        return null;
    }

    public void process(CoprocessorSettingsMap coprocessorSettingsMap) {
        this.coprocessorSettingsMap = coprocessorSettingsMap;
    }

    public void coprocessorMap(Map<CoprocessorSettingsMap.CoprocessorKey, Coprocessor> coprocessorMap) {
        this.coprocessorMap = coprocessorMap;
    }

    public void payloadMap(Map<CoprocessorSettingsMap.CoprocessorKey, Payload> payloadMap) {
        this.payloadMap = payloadMap;
    }
}
