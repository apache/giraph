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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.aggregators.WorkerAggregatorRequestProcessor;
import org.apache.giraph.comm.aggregators.AggregatedValueOutputStream;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.comm.aggregators.AllAggregatorServerData;
import org.apache.giraph.comm.aggregators.OwnerAggregatorServerData;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Handler for aggregators on worker. Provides the aggregated values and
 * performs aggregations from user vertex code (thread-safe). Also has
 * methods for all superstep coordination related to aggregators.
 *
 * At the beginning of any superstep any worker calls prepareSuperstep(),
 * which blocks until the final aggregates from the previous superstep have
 * been delivered to the worker.
 * Next, during the superstep worker can call aggregate() and
 * getAggregatedValue() (both methods are thread safe) the former
 * computes partial aggregates for this superstep from the worker,
 * the latter returns (read-only) final aggregates from the previous superstep.
 * Finally, at the end of the superstep, the worker calls finishSuperstep(),
 * which propagates non-owned partial aggregates to the owner workers,
 * and sends the final aggregate from the owner worker to the master.
 */
public class WorkerAggregatorHandler implements WorkerThreadAggregatorUsage {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(WorkerAggregatorHandler.class);
  /** Map of values from previous superstep */
  private Map<String, Writable> previousAggregatedValueMap =
      Maps.newHashMap();
  /** Map of aggregators for current superstep */
  private Map<String, Aggregator<Writable>> currentAggregatorMap =
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
  public <A extends Writable> void aggregate(String name, A value) {
    Aggregator<Writable> aggregator = currentAggregatorMap.get(name);
    if (aggregator != null) {
      progressable.progress();
      synchronized (aggregator) {
        aggregator.aggregate(value);
      }
    } else {
      throw new IllegalStateException("aggregate: " +
          AggregatorUtils.getUnregisteredAggregatorMessage(name,
              currentAggregatorMap.size() != 0, conf));
    }
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    A value = (A) previousAggregatedValueMap.get(name);
    if (value == null) {
      LOG.warn("getAggregatedValue: " +
          AggregatorUtils.getUnregisteredAggregatorMessage(name,
              previousAggregatedValueMap.size() != 0, conf));
    }
    return value;
  }

  /**
   * Prepare aggregators for current superstep
   *
   * @param requestProcessor Request processor for aggregators
   */
  public void prepareSuperstep(
      WorkerAggregatorRequestProcessor requestProcessor) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Start preparing aggregators");
    }
    AllAggregatorServerData allAggregatorData =
        serviceWorker.getServerData().getAllAggregatorData();
    // Wait for my aggregators
    Iterable<byte[]> dataToDistribute =
        allAggregatorData.getDataFromMasterWhenReady(
            serviceWorker.getMasterInfo());
    try {
      // Distribute my aggregators
      requestProcessor.distributeAggregators(dataToDistribute);
    } catch (IOException e) {
      throw new IllegalStateException("prepareSuperstep: " +
          "IOException occurred while trying to distribute aggregators", e);
    }
    // Wait for all other aggregators and store them
    allAggregatorData.fillNextSuperstepMapsWhenReady(
        getOtherWorkerIdsSet(), previousAggregatedValueMap,
        currentAggregatorMap);
    allAggregatorData.reset();
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
    OwnerAggregatorServerData ownerAggregatorData =
        serviceWorker.getServerData().getOwnerAggregatorData();
    // First send partial aggregated values to their owners and determine
    // which aggregators belong to this worker
    for (Map.Entry<String, Aggregator<Writable>> entry :
        currentAggregatorMap.entrySet()) {
      try {
        boolean sent = requestProcessor.sendAggregatedValue(entry.getKey(),
            entry.getValue().getAggregatedValue());
        if (!sent) {
          // If it's my aggregator, add it directly
          ownerAggregatorData.aggregate(entry.getKey(),
              entry.getValue().getAggregatedValue());
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
    Iterable<Map.Entry<String, Writable>> myAggregators =
        ownerAggregatorData.getMyAggregatorValuesWhenReady(
            getOtherWorkerIdsSet());

    // Send final aggregated values to master
    AggregatedValueOutputStream aggregatorOutput =
        new AggregatedValueOutputStream();
    for (Map.Entry<String, Writable> entry : myAggregators) {
      try {
        int currentSize = aggregatorOutput.addAggregator(entry.getKey(),
            entry.getValue());
        if (currentSize > maxBytesPerAggregatorRequest) {
          requestProcessor.sendAggregatedValuesToMaster(
              aggregatorOutput.flush());
        }
        progressable.progress();
      } catch (IOException e) {
        throw new IllegalStateException("finishSuperstep: " +
            "IOException occurred while writing aggregator " +
            entry.getKey(), e);
      }
    }
    try {
      requestProcessor.sendAggregatedValuesToMaster(aggregatorOutput.flush());
    } catch (IOException e) {
      throw new IllegalStateException("finishSuperstep: " +
          "IOException occured while sending aggregators to master", e);
    }
    // Wait for master to receive aggregated values before proceeding
    serviceWorker.getWorkerClient().waitAllRequests();

    ownerAggregatorData.reset();
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
  public WorkerThreadAggregatorUsage newThreadAggregatorUsage() {
    if (AggregatorUtils.useThreadLocalAggregators(conf)) {
      return new ThreadLocalWorkerAggregatorUsage();
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
   * Not thread-safe implementation of {@link WorkerThreadAggregatorUsage}.
   * We can use one instance of this object per thread to prevent
   * synchronizing on each aggregate() call. In the end of superstep,
   * values from each of these will be aggregated back to {@link
   * WorkerAggregatorHandler}
   */
  public class ThreadLocalWorkerAggregatorUsage
      implements WorkerThreadAggregatorUsage {
    /** Thread-local aggregator map */
    private final Map<String, Aggregator<Writable>> threadAggregatorMap;

    /**
     * Constructor
     *
     * Creates new instances of all aggregators from
     * {@link WorkerAggregatorHandler}
     */
    public ThreadLocalWorkerAggregatorUsage() {
      threadAggregatorMap = Maps.newHashMapWithExpectedSize(
          WorkerAggregatorHandler.this.currentAggregatorMap.size());
      for (Map.Entry<String, Aggregator<Writable>> entry :
          WorkerAggregatorHandler.this.currentAggregatorMap.entrySet()) {
        threadAggregatorMap.put(entry.getKey(),
            AggregatorUtils.newAggregatorInstance(
                (Class<Aggregator<Writable>>) entry.getValue().getClass(),
                conf));
      }
    }

    @Override
    public <A extends Writable> void aggregate(String name, A value) {
      Aggregator<Writable> aggregator = threadAggregatorMap.get(name);
      if (aggregator != null) {
        progressable.progress();
        aggregator.aggregate(value);
      } else {
        throw new IllegalStateException("aggregate: " +
            AggregatorUtils.getUnregisteredAggregatorMessage(name,
                threadAggregatorMap.size() != 0, conf));
      }
    }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
      return WorkerAggregatorHandler.this.<A>getAggregatedValue(name);
    }

    @Override
    public void finishThreadComputation() {
      // Aggregate the values this thread's vertices provided back to
      // WorkerAggregatorHandler
      for (Map.Entry<String, Aggregator<Writable>> entry :
          threadAggregatorMap.entrySet()) {
        WorkerAggregatorHandler.this.aggregate(entry.getKey(),
            entry.getValue().getAggregatedValue());
      }
    }
  }
}
