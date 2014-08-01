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

package org.apache.giraph.comm.aggregators;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.master.MasterInfo;
import org.apache.giraph.utils.Factory;
import org.apache.giraph.utils.TaskIdsPermitsBarrier;
import org.apache.giraph.utils.WritableFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Accepts aggregators and their values from previous superstep from master
 * and workers which own aggregators. Keeps data received from master so it
 * could be distributed later. Also counts the requests so we would know
 * when we are done receiving requests.
 *
 * Only restriction is that we need to call registerAggregatorClass before
 * calling createAggregatorInitialValue, other than that methods of this class
 * are thread-safe.
 */
public class AllAggregatorServerData {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(AllAggregatorServerData.class);
  /** Map of aggregator factories */
  private final ConcurrentMap<String, WritableFactory<Aggregator<Writable>>>
  aggregatorFactoriesMap = Maps.newConcurrentMap();
  /** Map of values of aggregators from previous superstep */
  private final ConcurrentMap<String, Writable>
  aggregatedValuesMap = Maps.newConcurrentMap();
  /**
   * Counts the requests with final aggregators from master.
   * It uses values from special aggregators
   * (named AggregatorUtils.SPECIAL_COUNT_AGGREGATOR)
   * to know how many requests it has to receive.
   */
  private final TaskIdsPermitsBarrier masterBarrier;
  /**
   * Aggregator data which this worker received from master and which it is
   * going to distribute before starting next superstep. Thread-safe.
   */
  private final List<byte[]> masterData =
      Collections.synchronizedList(Lists.<byte[]>newArrayList());
  /**
   * Counts the requests with final aggregators from other workers.
   * It uses values from special aggregators
   * (named AggregatorUtils.SPECIAL_COUNT_AGGREGATOR)
   * to know how many requests it has to receive.
   */
  private final TaskIdsPermitsBarrier workersBarrier;
  /** Progressable used to report progress */
  private final Progressable progressable;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor
   *
   * @param progressable Progressable used to report progress
   * @param conf Configuration
   */
  public AllAggregatorServerData(Progressable progressable,
      ImmutableClassesGiraphConfiguration conf) {
    this.progressable = progressable;
    this.conf = conf;
    workersBarrier = new TaskIdsPermitsBarrier(progressable);
    masterBarrier = new TaskIdsPermitsBarrier(progressable);
  }

  /**
   * Register the class of the aggregator, received by master or worker.
   *
   * @param name              Aggregator name
   * @param aggregatorFactory Aggregator factory
   */
  public void registerAggregatorClass(String name,
      WritableFactory<Aggregator<Writable>> aggregatorFactory) {
    aggregatorFactoriesMap.put(name, aggregatorFactory);
    progressable.progress();
  }

  /**
   * Set the value of aggregator from previous superstep,
   * received by master or worker.
   *
   * @param name Name of the aggregator
   * @param value Value of the aggregator
   */
  public void setAggregatorValue(String name, Writable value) {
    aggregatedValuesMap.put(name, value);
    progressable.progress();
  }

  /**
   * Create initial aggregated value for an aggregator. Used so requests
   * would be able to deserialize data.
   * registerAggregatorClass needs to be called first to ensure that we have
   * the class of the aggregator.
   *
   * @param name Name of the aggregator
   * @return Empty aggregated value for this aggregator
   */
  public Writable createAggregatorInitialValue(String name) {
    WritableFactory<Aggregator<Writable>> aggregatorFactory =
        aggregatorFactoriesMap.get(name);
    synchronized (aggregatorFactory) {
      return aggregatorFactory.create().createInitialValue();
    }
  }

  /**
   * Notify this object that an aggregator request from master has been
   * received.
   *
   * @param aggregatorData Byte request with data received from master
   */
  public void receivedRequestFromMaster(byte[] aggregatorData) {
    masterData.add(aggregatorData);
    masterBarrier.releaseOnePermit();
  }

  /**
   * Notify this object about the total number of requests which should
   * arrive from master.
   *
   * @param requestCount Number of requests which should arrive
   * @param taskId Task id of master
   */
  public void receivedRequestCountFromMaster(long requestCount, int taskId) {
    masterBarrier.requirePermits(requestCount, taskId);
  }

  /**
   * Notify this object that an aggregator request from some worker has been
   * received.
   */
  public void receivedRequestFromWorker() {
    workersBarrier.releaseOnePermit();
  }

  /**
   * Notify this object about the total number of requests which should
   * arrive from one of the workers.
   *
   * @param requestCount Number of requests which should arrive
   * @param taskId Task id of that worker
   */
  public void receivedRequestCountFromWorker(long requestCount, int taskId) {
    workersBarrier.requirePermits(requestCount, taskId);
  }

  /**
   * This function will wait until all aggregator requests from master have
   * arrived, and return that data afterwards.
   *
   * @param masterInfo Master info
   * @return Iterable through data received from master
   */
  public Iterable<byte[]> getDataFromMasterWhenReady(MasterInfo masterInfo) {
    masterBarrier.waitForRequiredPermits(
        Collections.singleton(masterInfo.getTaskId()));
    if (LOG.isDebugEnabled()) {
      LOG.debug("getDataFromMasterWhenReady: " +
          "Aggregator data for distribution ready");
    }
    return masterData;
  }

  /**
   * This function will wait until all aggregator requests from workers have
   * arrived, and fill the maps for next superstep when ready.
   *
   * @param workerIds All workers in the job apart from the current one
   * @param previousAggregatedValuesMap Map of values from previous
   *                                    superstep to fill out
   * @param currentAggregatorFactoryMap Map of aggregators factories for
   *                                    current superstep to fill out.
   */
  public void fillNextSuperstepMapsWhenReady(
      Set<Integer> workerIds,
      Map<String, Writable> previousAggregatedValuesMap,
      Map<String, Factory<Aggregator<Writable>>> currentAggregatorFactoryMap) {
    workersBarrier.waitForRequiredPermits(workerIds);
    if (LOG.isDebugEnabled()) {
      LOG.debug("fillNextSuperstepMapsWhenReady: Aggregators ready");
    }
    previousAggregatedValuesMap.clear();
    previousAggregatedValuesMap.putAll(aggregatedValuesMap);
    for (Map.Entry<String, WritableFactory<Aggregator<Writable>>> entry :
        aggregatorFactoriesMap.entrySet()) {
      Factory<Aggregator<Writable>> aggregatorFactory =
          currentAggregatorFactoryMap.get(entry.getKey());
      if (aggregatorFactory == null) {
        currentAggregatorFactoryMap.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Prepare for next superstep
   */
  public void reset() {
    masterData.clear();
    if (LOG.isDebugEnabled()) {
      LOG.debug("reset: Ready for next superstep");
    }
  }
}

