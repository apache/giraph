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

package org.apache.giraph.graph;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * Balancer that automatically balances vertex ranges based on
 * number of vertices or edges (configurable).
 *
 * @param <I> vertex id type
 */
@SuppressWarnings("rawtypes")
public final class AutoBalancer<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends VertexRangeBalancer<I, V, E, M> {

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(AutoBalancer.class);

    /** enum for what to balance on */
    public enum BalCriterium {
        BAL_NONE,
        BAL_NUM_VERTICES,
        BAL_NUM_EDGES,
        BAL_NUM_VERTICES_AND_EDGES,
    }

    /** boolean: decides whether to balance number of vertices or edges */
    private static BalCriterium balanceOn =
        BalCriterium.BAL_NUM_VERTICES_AND_EDGES;

    public static void setBalCriterium(BalCriterium balanceOn) {
        AutoBalancer.balanceOn = balanceOn;
    }

    private long getVertexRangeEntries(VertexRange<I,V,E,M> v)
    {
        if (balanceOn == BalCriterium.BAL_NUM_VERTICES_AND_EDGES) {
            return v.getVertexCount() + v.getEdgeCount();
        }
        if (balanceOn == BalCriterium.BAL_NUM_EDGES) {
            return v.getEdgeCount();
        }
        if (balanceOn == BalCriterium.BAL_NUM_VERTICES) {
            return v.getVertexCount();
        }
        return 0;
    }

    public class VertexRangeComparator
            implements Comparator<VertexRange<I,V,E,M>> {
        /**
         * Compares the number of entities based on balanceOn.
         */
        @SuppressWarnings("unchecked")
        public int compare(VertexRange<I,V,E,M> v1, VertexRange<I,V,E,M> v2) {
            long numEntries1 = getVertexRangeEntries(v1);
            long numEntries2 = getVertexRangeEntries(v2);
            if (numEntries1 == numEntries2) {
                return v1.getMaxIndex().compareTo(v2.getMaxIndex());
            }
            return (int)(numEntries2 - numEntries1);
        }
    }

    @Override
    public final NavigableMap<I, VertexRange<I, V, E, M>> rebalance() {
        if (balanceOn == BalCriterium.BAL_NONE) {
            return getPrevVertexRangeMap();
        }

        Map<String, JSONArray> workerHostnameIdMap = getWorkerHostnamePortMap();
        NavigableMap<I, VertexRange<I, V, E, M>> prevVertexRangeMap =
            getPrevVertexRangeMap();
        NavigableMap<I, VertexRange<I, V, E, M>> nextVertexRangeMap =
            new TreeMap<I, VertexRange<I, V, E, M>>();
        NavigableMap<String, Set<VertexRange<I,V,E,M>>> hostToVertexRangeMap =
            new TreeMap<String, Set<VertexRange<I, V, E, M>>>();
        NavigableMap<String, LongWritable> hostToNumEntriesMap =
            new TreeMap<String, LongWritable>();
        NavigableMap<VertexRange<I,V,E,M>, String> vRngDistToHostMap =
            new TreeMap<VertexRange<I, V, E, M>, String>(
                    new VertexRangeComparator());
        NavigableMap<LongWritable, List<String>> numEntriesToHostMap =
            new TreeMap<LongWritable, List<String>>();
        LOG.info("rebalance: workerHostnameIdMap size=" +
                 workerHostnameIdMap.size() +
                 " prevVertexRangeMap size=" +
                 prevVertexRangeMap.size());

        // First generate total number of entries to balance on
        // and generate appropriate maps:
        // map from hostnameId to list of vertex ranges
        // map from hostnameId to total number of entries
        int numVertexRanges = 0;
        long numTotalEntries = 0;
        // initialize with workerHostnameIdMap to account for
        // hostnameId's without any vertices
        for (String hostnameId : workerHostnameIdMap.keySet()) {
            hostToNumEntriesMap.put(hostnameId, new LongWritable(0));
            hostToVertexRangeMap.put(hostnameId,
                    new TreeSet<VertexRange<I, V, E, M>>(
                        new VertexRangeComparator()));
        }
        for (Entry<I, VertexRange<I, V, E, M>> entry :
                prevVertexRangeMap.entrySet()) {
            numVertexRanges++;
            VertexRange<I, V, E, M> vRange = entry.getValue();
            long numEntries = getVertexRangeEntries(vRange);
            numTotalEntries += numEntries;
            String hostnameId = vRange.getHostnameId();
            LongWritable entryLong = hostToNumEntriesMap.get(hostnameId);
            if (entryLong == null) {
                throw new RuntimeException("Unknown hostnameId=" +
                        hostnameId);
            }
            entryLong.set(entryLong.get() + numEntries);
            Set<VertexRange<I, V, E, M>> vRangeList =
                hostToVertexRangeMap.get(hostnameId);
            if (vRangeList == null) {
                throw new RuntimeException("Unknown hostnameId=" +
                        hostnameId);
            }
            if (vRangeList.add(vRange) == false) {
                throw new RuntimeException(
                        "All VertexRanges should be different");
            }
        }
        LOG.info("rebalance: numTotalEntries=" + numTotalEntries +
                 " numVertexRanges=" + numVertexRanges +
                 " hostToVertexRangeMap size=" + hostToVertexRangeMap.size() +
                 " hostToNumEntriesMap size=" + hostToNumEntriesMap.size());

        // Next take away the vertex ranges with the smallest number of
        // entities till every hostnameId maps to  a list of vertex ranges
        // containing not more than the average number of entries.
        // The vertex ranges taken away are put as keys into a sorted map,
        // sorted on the number of entries in descending order.
        long aveWorkerEntries = numTotalEntries / hostToNumEntriesMap.size();
        long squareDeviation = 0;
        numVertexRanges = 0;
        numTotalEntries = 0;
        for (Entry<String, Set<VertexRange<I,V,E,M>>> entry :
                hostToVertexRangeMap.entrySet()) {
            long numEntries = hostToNumEntriesMap.get(entry.getKey()).get();
            numTotalEntries += numEntries;
            numVertexRanges += entry.getValue().size();
            squareDeviation += ((numEntries - aveWorkerEntries) *
                                (numEntries - aveWorkerEntries));
            if (numEntries > aveWorkerEntries) {
                int sz = entry.getValue().size();
                for (int i = 0; i < sz; i++) {
                    VertexRange<I,V,E,M> vRange =
                        ((TreeSet<VertexRange<I,V,E,M>>)
                             entry.getValue()).pollLast();
                    vRngDistToHostMap.put(vRange, entry.getKey());
                    numEntries -= getVertexRangeEntries(vRange);
                    if (numEntries <= aveWorkerEntries) {
                        hostToNumEntriesMap.get(entry.getKey()).set(numEntries);
                        break;
                    }
                }
            }
        }
        LOG.info("rebalance: Initial squareDeviation=" + squareDeviation +
                 " numTotalEntries=" + numTotalEntries +
                 " numVertexRanges=" + numVertexRanges);

        // Next the map from hostnameId to number of entries is reversed
        // (value becomes key) such that the number of entries get sorted.
        // Then the removed vertex ranges are assigned to hostnameId's, largest
        // vertex ranges to hostnameId's with the smallest number of entities.
        for (Entry<String, LongWritable> entry :
                hostToNumEntriesMap.entrySet()) {
            List<String> hostnameIds =
                numEntriesToHostMap.get(entry.getValue());
            if (hostnameIds == null) {
                hostnameIds = new ArrayList<String>();
                numEntriesToHostMap.put(entry.getValue(), hostnameIds);
            }
            hostnameIds.add(entry.getKey());
        }

        for (Entry<VertexRange<I,V,E,M>, String> entry :
                vRngDistToHostMap.entrySet()) {
            String hostnameId = entry.getValue();
            String newHostnameId = null;
            Entry<LongWritable, List<String>> entriesToHost =
                numEntriesToHostMap.firstEntry();
            for (String id : entriesToHost.getValue()) {
                if (hostnameId.equals(id)) {
                    newHostnameId = id;
                    break;
                }
            }
            if (newHostnameId == null) {
                newHostnameId = entriesToHost.getValue().get(0);
            }
            entriesToHost.getValue().remove(newHostnameId);
            VertexRange<I, V, E, M> replacedVertexRange = null;
            if (hostnameId.equals(newHostnameId)) {
                replacedVertexRange = entry.getKey();
            } else {
                try {
                    replacedVertexRange =
                        new VertexRange<I, V, E, M>(entry.getKey());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                JSONArray hostnamePortArray =
                    workerHostnameIdMap.get(newHostnameId);
                try {
                    replacedVertexRange.setHostnameId(newHostnameId);
                    replacedVertexRange.setHostname(
                            hostnamePortArray.getString(0));
                    replacedVertexRange.setPort(hostnamePortArray.getInt(1));
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            }
            nextVertexRangeMap.put(replacedVertexRange.getMaxIndex(),
                    replacedVertexRange);

            LongWritable numEntries =
                new LongWritable(entriesToHost.getKey().get() +
                        getVertexRangeEntries(entry.getKey()));
            if (entriesToHost.getValue().size() == 0) {
                numEntriesToHostMap.remove(entriesToHost.getKey());
            }
            List<String> hostnameIds = numEntriesToHostMap.get(numEntries);
            if (hostnameIds == null) {
                hostnameIds = new ArrayList<String>();
                numEntriesToHostMap.put(numEntries, hostnameIds);
            }
            hostnameIds.add(newHostnameId);
        }
        squareDeviation = 0;
        numTotalEntries = 0;
        numVertexRanges = 0;
        for (Entry<LongWritable, List<String>> entriesToHost :
                numEntriesToHostMap.entrySet()) {
            long numEntries = entriesToHost.getKey().get();
            squareDeviation += (entriesToHost.getValue().size() *
                               (numEntries - aveWorkerEntries) *
                               (numEntries - aveWorkerEntries));
            numTotalEntries += (numEntries * entriesToHost.getValue().size());
            numVertexRanges += entriesToHost.getValue().size();
        }
        for (Set<VertexRange<I,V,E,M>> vRangeSet :
                hostToVertexRangeMap.values()) {
            for (VertexRange<I,V,E,M> vRange : vRangeSet) {
                nextVertexRangeMap.put(vRange.getMaxIndex(), vRange);
            }
        }
        LOG.info("rebalance: Final squareDeviation=" + squareDeviation +
                 " numTotalEntries=" + numTotalEntries +
                 " numVertexRangesCalculated=" + numVertexRanges +
                 " numVertexRangesAssigned=" + nextVertexRangeMap.size());

        // only run once, revisit this when we make graph mutations
        balanceOn = BalCriterium.BAL_NONE;

        return nextVertexRangeMap;
    }
}
