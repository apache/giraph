/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import org.apache.giraph.graph.VertexRangeBalancer;
import org.apache.giraph.graph.VertexRange;

@SuppressWarnings("rawtypes")
public class SuperstepBalancer<I extends WritableComparable,
                               V extends Writable,
                               E extends Writable,
                               M extends Writable>
                               extends VertexRangeBalancer<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(SuperstepBalancer.class);

    public NavigableMap<I, VertexRange<I, V, E, M>> rebalance() {
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
        return nextVertexRangeMap;
    }
}
