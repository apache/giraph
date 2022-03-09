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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.comm.GlobalCommType;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.master.MasterInfo;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.Reducer;
import org.apache.giraph.utils.TaskIdsPermitsBarrier;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
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
  /** Map of broadcasted values from master */
  private final ConcurrentMap<String, Writable>
  broadcastedMap = Maps.newConcurrentMap();
  /** Map of registered reducers for current superstep */
  private final ConcurrentMap<String, ReduceOperation<Object, Writable>>
  reduceOpMap = Maps.newConcurrentMap();
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
   * Received value through global communication from master.
   * @param name Name
   * @param type Global communication type
   * @param value Object value
   */
  public void receiveValueFromMaster(
      String name, GlobalCommType type, Writable value) {
    switch (type) {
    case BROADCAST:
      broadcastedMap.put(name, value);
      break;

    case REDUCE_OPERATIONS:
      reduceOpMap.put(name, (ReduceOperation<Object, Writable>) value);
      break;

    default:
      throw new IllegalArgumentException("Unkown request type " + type);
    }
    progressable.progress();
  }

  /**
   * Notify this object that an aggregator request from master has been
   * received.
   *
   * @param data Byte request with data received from master
   */
  public void receivedRequestFromMaster(byte[] data) {
    masterData.add(data);
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
   * @param broadcastedMapToFill Broadcast map to fill out
   * @param reducerMapToFill Registered reducer map to fill out.
   */
  public void fillNextSuperstepMapsWhenReady(
      Set<Integer> workerIds,
      Map<String, Writable> broadcastedMapToFill,
      Map<String, Reducer<Object, Writable>> reducerMapToFill) {
    workersBarrier.waitForRequiredPermits(workerIds);
    if (LOG.isDebugEnabled()) {
      LOG.debug("fillNextSuperstepMapsWhenReady: Global data ready");
    }

    Preconditions.checkArgument(broadcastedMapToFill.isEmpty(),
        "broadcastedMap needs to be empty for filling");
    Preconditions.checkArgument(reducerMapToFill.isEmpty(),
        "reducerMap needs to be empty for filling");

    broadcastedMapToFill.putAll(broadcastedMap);

    for (Entry<String, ReduceOperation<Object, Writable>> entry :
        reduceOpMap.entrySet()) {
      reducerMapToFill.put(entry.getKey(), new Reducer<>(entry.getValue()));
    }

    broadcastedMap.clear();
    reduceOpMap.clear();
    masterData.clear();
    if (LOG.isDebugEnabled()) {
      LOG.debug("reset: Ready for next superstep");
    }
  }
}

