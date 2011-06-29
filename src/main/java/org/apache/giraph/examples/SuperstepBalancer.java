package org.apache.giraph.examples;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import org.apache.giraph.BspBalancer;
import org.apache.giraph.VertexRange;
import org.apache.giraph.VertexRangeBalancer;

@SuppressWarnings("rawtypes")
public class SuperstepBalancer<I extends WritableComparable,
                               V extends Writable,
                               E extends Writable,
                               M extends Writable>
                               extends BspBalancer<I, V, E, M>
                               implements VertexRangeBalancer<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(SuperstepBalancer.class);

    public void rebalance() {
        // Simple test to put all the vertex ranges on the superstep worker
        Map<String, JSONArray> workerHostnameIdMap = getWorkerHostnamePortMap();
        long hostnameIdListIndex = getSuperstep() % workerHostnameIdMap.size();
        String superstepWorker =
            (String) workerHostnameIdMap.keySet().toArray()[
                (int) hostnameIdListIndex];
        LOG.info("rebalance: Using worker " + superstepWorker + " with index " +
                 hostnameIdListIndex + " out of " +
                 workerHostnameIdMap.size() + " workers on superstep " +
                 getSuperstep());
        JSONArray hostnamePortArray = workerHostnameIdMap.get(superstepWorker);
        NavigableMap<I, VertexRange<I, V, E, M>> prevVertexRangeMap =
            getPrevVertexRangeMap();
        NavigableMap<I, VertexRange<I, V, E, M>> nextVertexRangeMap =
            new TreeMap<I, VertexRange<I, V, E, M>>();
        for (Entry<I, VertexRange<I, V, E, M>> entry :
                prevVertexRangeMap.entrySet()) {
            VertexRange<I, V, E, M> replacedVertexRange = null;
            try {
                replacedVertexRange =
                    new VertexRange<I, V, E, M>(entry.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                replacedVertexRange.setHostname(hostnamePortArray.getString(0));
                replacedVertexRange.setPort(hostnamePortArray.getInt(1));
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
            nextVertexRangeMap.put(replacedVertexRange.getMaxIndex(),
                                   replacedVertexRange);
        }
        setNextVertexRangeMap(nextVertexRangeMap);
    }

}
