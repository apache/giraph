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
package org.apache.giraph.worker;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.GlobalCommType;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.comm.aggregators.AllAggregatorServerData;
import org.apache.giraph.comm.aggregators.GlobalCommValueOutputStream;
import org.apache.giraph.comm.aggregators.OwnerAggregatorServerData;
import org.apache.giraph.comm.aggregators.WorkerAggregatorRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.Reducer;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/** Handler for reduce/broadcast on the workers */
public class WorkerAggregatorHandler implements WorkerThreadGlobalCommUsage {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(WorkerAggregatorHandler.class);
  /** Map of broadcasted values */
  private final Map<String, Writable> broadcastedMap =
      Maps.newHashMap();
  /** Map of reducers currently being reduced */
  private final Map<String, Reducer<Object, Writable>> reducerMap =
      Maps.newHashMap();

  /** Service worker */
  private final CentralizedServiceWorker<?, ?, ?> serviceWorker;
  /** Progressable for reporting progress */
  private final Progressable progressable;
  /** How big a single aggregator request can be */
  private final int maxBytesPerAggregatorRequest;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor
   *
   * @param serviceWorker Service worker
   * @param conf          Giraph configuration
   * @param progressable  Progressable for reporting progress
   */
  public WorkerAggregatorHandler(
      CentralizedServiceWorker<?, ?, ?> serviceWorker,
      ImmutableClassesGiraphConfiguration conf,
      Progressable progressable) {
    this.serviceWorker = serviceWorker;
    this.progressable = progressable;
    this.conf = conf;
    maxBytesPerAggregatorRequest = conf.getInt(
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST,
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST_DEFAULT);
  }

  @Override
  public <B extends Writable> B getBroadcast(String name) {
    B value = (B) broadcastedMap.get(name);
    if (value == null) {
      LOG.warn("getBroadcast: " +
          AggregatorUtils.getUnregisteredBroadcastMessage(name,
              broadcastedMap.size() != 0, conf));
    }
    return value;
  }

  @Override
  public void reduce(String name, Object value) {
    Reducer<Object, Writable> reducer = reducerMap.get(name);
    if (reducer != null) {
      progressable.progress();
      synchronized (reducer) {
        reducer.reduce(value);
      }
    } else {
      throw new IllegalStateException("reduce: " +
          AggregatorUtils.getUnregisteredReducerMessage(name,
              reducerMap.size() != 0, conf));
    }
  }

  /**
   * Combine partially reduced value into currently reduced value.
   * @param name Name of the reducer
   * @param valueToReduce Partial value to reduce
   */
  @Override
  public void reduceMerge(String name, Writable valueToReduce) {
    Reducer<Object, Writable> reducer = reducerMap.get(name);
    if (reducer != null) {
      progressable.progress();
      synchronized (reducer) {
        reducer.reduceMerge(valueToReduce);
      }
    } else {
      throw new IllegalStateException("reduce: " +
          AggregatorUtils.getUnregisteredReducerMessage(name,
              reducerMap.size() != 0, conf));
    }
  }

  /**
   * Prepare aggregators for current superstep
   *
   * @param requestProcessor Request processor for aggregators
   */
  public void prepareSuperstep(
      WorkerAggregatorRequestProcessor requestProcessor) {
    broadcastedMap.clear();
    reducerMap.clear();

    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Start preparing aggregators");
    }
    AllAggregatorServerData allGlobalCommData =
        serviceWorker.getServerData().getAllAggregatorData();
    // Wait for my aggregators
    Iterable<byte[]> dataToDistribute =
        allGlobalCommData.getDataFromMasterWhenReady(
            serviceWorker.getMasterInfo());
    try {
      // Distribute my aggregators
      requestProcessor.distributeReducedValues(dataToDistribute);
    } catch (IOException e) {
      throw new IllegalStateException("prepareSuperstep: " +
          "IOException occurred while trying to distribute aggregators", e);
    }
    // Wait for all other aggregators and store them
    allGlobalCommData.fillNextSuperstepMapsWhenReady(
        getOtherWorkerIdsSet(), broadcastedMap,
        reducerMap);
    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Aggregators prepared");
    }
  }

  /**
   * Send aggregators to their owners and in the end to the master
   *
   * @param requestProcessor Request processor for aggregators
   */
  public void finishSuperstep(
      WorkerAggregatorRequestProcessor requestProcessor) {
    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Start gathering aggregators, " +
          "workers will send their aggregated values " +
          "once they are done with superstep computation");
    }
    OwnerAggregatorServerData ownerGlobalCommData =
        serviceWorker.getServerData().getOwnerAggregatorData();
    // First send partial aggregated values to their owners and determine
    // which aggregators belong to this worker
    for (Map.Entry<String, Reducer<Object, Writable>> entry :
        reducerMap.entrySet()) {
      try {
        boolean sent = requestProcessor.sendReducedValue(entry.getKey(),
            entry.getValue().getCurrentValue());
        if (!sent) {
          // If it's my aggregator, add it directly
          ownerGlobalCommData.reduce(entry.getKey(),
              entry.getValue().getCurrentValue());
        }
      } catch (IOException e) {
        throw new IllegalStateException("finishSuperstep: " +
            "IOException occurred while sending aggregator " +
            entry.getKey() + " to its owner", e);
      }
      progressable.progress();
    }
    try {
      // Flush
      requestProcessor.flush();
    } catch (IOException e) {
      throw new IllegalStateException("finishSuperstep: " +
          "IOException occurred while sending aggregators to owners", e);
    }

    // Wait to receive partial aggregated values from all other workers
    Iterable<Map.Entry<String, Writable>> myReducedValues =
        ownerGlobalCommData.getMyReducedValuesWhenReady(
            getOtherWorkerIdsSet());

    // Send final aggregated values to master
    GlobalCommValueOutputStream globalOutput =
        new GlobalCommValueOutputStream(false);
    for (Map.Entry<String, Writable> entry : myReducedValues) {
      try {
        int currentSize = globalOutput.addValue(entry.getKey(),
            GlobalCommType.REDUCED_VALUE,
            entry.getValue());
        if (currentSize > maxBytesPerAggregatorRequest) {
          requestProcessor.sendReducedValuesToMaster(
              globalOutput.flush());
        }
        progressable.progress();
      } catch (IOException e) {
        throw new IllegalStateException("finishSuperstep: " +
            "IOException occurred while writing aggregator " +
            entry.getKey(), e);
      }
    }
    try {
      requestProcessor.sendReducedValuesToMaster(globalOutput.flush());
    } catch (IOException e) {
      throw new IllegalStateException("finishSuperstep: " +
          "IOException occured while sending aggregators to master", e);
    }
    // Wait for master to receive aggregated values before proceeding
    serviceWorker.getWorkerClient().waitAllRequests();

    ownerGlobalCommData.reset();
    if (LOG.isDebugEnabled()) {
      LOG.debug("finishSuperstep: Aggregators finished");
    }
  }

  /**
   * Create new aggregator usage which will be used by one of the compute
   * threads.
   *
   * @return New aggregator usage
   */
  public WorkerThreadGlobalCommUsage newThreadAggregatorUsage() {
    if (AggregatorUtils.useThreadLocalAggregators(conf)) {
      return new ThreadLocalWorkerGlobalCommUsage();
    } else {
      return this;
    }
  }

  @Override
  public void finishThreadComputation() {
    // If we don't use thread-local aggregators, all the aggregated values
    // are already in this object
  }

  /**
   * Get set of all worker task ids except the current one
   *
   * @return Set of all other worker task ids
   */
  public Set<Integer> getOtherWorkerIdsSet() {
    Set<Integer> otherWorkers = Sets.newHashSetWithExpectedSize(
        serviceWorker.getWorkerInfoList().size());
    for (WorkerInfo workerInfo : serviceWorker.getWorkerInfoList()) {
      if (workerInfo.getTaskId() != serviceWorker.getWorkerInfo().getTaskId()) {
        otherWorkers.add(workerInfo.getTaskId());
      }
    }
    return otherWorkers;
  }

  /**
  * Not thread-safe implementation of {@link WorkerThreadGlobalCommUsage}.
  * We can use one instance of this object per thread to prevent
  * synchronizing on each aggregate() call. In the end of superstep,
  * values from each of these will be aggregated back to {@link
  * WorkerThreadGlobalCommUsage}
  */
  public class ThreadLocalWorkerGlobalCommUsage
    implements WorkerThreadGlobalCommUsage {
    /** Thread-local reducer map */
    private final Map<String, Reducer<Object, Writable>> threadReducerMap;

    /**
    * Constructor
    *
    * Creates new instances of all reducers from
    * {@link WorkerAggregatorHandler}
    */
    public ThreadLocalWorkerGlobalCommUsage() {
      threadReducerMap = Maps.newHashMapWithExpectedSize(
          WorkerAggregatorHandler.this.reducerMap.size());

      UnsafeByteArrayOutputStream out = new UnsafeByteArrayOutputStream();
      UnsafeReusableByteArrayInput in = new UnsafeReusableByteArrayInput();

      for (Entry<String, Reducer<Object, Writable>> entry :
          reducerMap.entrySet()) {
        ReduceOperation<Object, Writable> globalReduceOp =
            entry.getValue().getReduceOp();

        ReduceOperation<Object, Writable> threadLocalCopy =
            WritableUtils.createCopy(out, in, globalReduceOp, conf);

        threadReducerMap.put(entry.getKey(), new Reducer<>(threadLocalCopy));
      }
    }

    @Override
    public void reduce(String name, Object value) {
      Reducer<Object, Writable> reducer = threadReducerMap.get(name);
      if (reducer != null) {
        progressable.progress();
        reducer.reduce(value);
      } else {
        throw new IllegalStateException("reduce: " +
            AggregatorUtils.getUnregisteredAggregatorMessage(name,
                threadReducerMap.size() != 0, conf));
      }
    }

    @Override
    public void reduceMerge(String name, Writable value) {
      Reducer<Object, Writable> reducer = threadReducerMap.get(name);
      if (reducer != null) {
        progressable.progress();
        reducer.reduceMerge(value);
      } else {
        throw new IllegalStateException("reduceMerge: " +
            AggregatorUtils.getUnregisteredAggregatorMessage(name,
                threadReducerMap.size() != 0, conf));
      }
    }

    @Override
    public <B extends Writable> B getBroadcast(String name) {
      return WorkerAggregatorHandler.this.getBroadcast(name);
    }

    @Override
    public void finishThreadComputation() {
      // Aggregate the values this thread's vertices provided back to
      // WorkerAggregatorHandler
      for (Entry<String, Reducer<Object, Writable>> entry :
          threadReducerMap.entrySet()) {
        WorkerAggregatorHandler.this.reduceMerge(entry.getKey(),
            entry.getValue().getCurrentValue());
      }
    }
  }

}
