/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.block_app.framework.api.local;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.master.MasterGlobalCommUsage;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.Reducer;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;

/**
 * Internal aggregators implementation
 */
@SuppressWarnings("unchecked")
class InternalAggregators
    implements MasterGlobalCommUsage, WorkerGlobalCommUsage {
  private final boolean runAllChecks;

  /** Map of reducers registered for the next worker computation */
  private final Map<String, Reducer<Object, Writable>> reducerMap =
      Maps.newHashMap();
  /** Map of values to be sent to workers for next computation */
  private final Map<String, Writable> broadcastMap =
      Maps.newHashMap();
  /** Values reduced from previous computation */
  private final Map<String, Writable> reducedMap =
      Maps.newHashMap();

  public InternalAggregators(boolean runAllChecks) {
    this.runAllChecks = runAllChecks;
  }

  private static <T> T getOrThrow(
      Map<String, T> map, String mapName, String key) {
    T value = map.get(key);
    if (value == null) {
      throw new IllegalArgumentException(
          key + " not present in " + mapName);
    }
    return value;
  }

  @Override
  public void broadcast(String name, Writable value) {
    broadcastMap.put(name, value);
  }

  @Override
  public <B extends Writable> B getBroadcast(String name) {
    return (B) getOrThrow(broadcastMap, "broadcastMap", name);
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp) {
    registerReducer(name, reduceOp, reduceOp.createInitialValue());
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp,
      R globalInitialValue) {
    if (reducerMap.containsKey(name)) {
      throw new IllegalArgumentException(
          "Reducer with name " + name + " was already registered, " +
          " and is " + reducerMap.get(name).getReduceOp() +
          ", and we are trying to " + " register " + reduceOp);
    }
    if (reduceOp == null) {
      throw new IllegalArgumentException(
          "null reducer cannot be registered, with name " + name);
    }
    if (globalInitialValue == null) {
      throw new IllegalArgumentException(
          "global initial value for reducer cannot be null, but is for " +
          reduceOp + " with naem" + name);
    }

    Reducer<S, R> reducer = new Reducer<>(reduceOp, globalInitialValue);
    reducerMap.put(name, (Reducer<Object, Writable>) reducer);
  }

  @Override
  public void reduce(String name, Object value) {
    Reducer<Object, Writable> reducer =
        getOrThrow(reducerMap, "reducerMap", name);
    synchronized (reducer) {
      reducer.reduce(value);
    }
  }

  @Override
  public void reduceMerge(String name, Writable value) {
    Reducer<Object, Writable> reducer =
        getOrThrow(reducerMap, "reducerMap", name);
    synchronized (reducer) {
      reducer.reduceMerge(value);
    }
  }

  @Override
  public <R extends Writable> R getReduced(String name) {
    return (R) getOrThrow(reducedMap, "reducedMap", name);
  }

  public synchronized void afterWorkerBeforeMaster() {
    broadcastMap.clear();
    reducedMap.clear();
    for (Entry<String, Reducer<Object, Writable>> entry :
          reducerMap.entrySet()) {
      Writable value = entry.getValue().getCurrentValue();
      if (runAllChecks) {
        Writable newValue = entry.getValue().createInitialValue();
        WritableUtils.copyInto(value, newValue);
        value = newValue;
      }
      reducedMap.put(entry.getKey(), value);
    }
    reducerMap.clear();
  }
}
