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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.Reducer;
import org.apache.giraph.utils.TaskIdsPermitsBarrier;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Class for holding aggregators which current worker owns,
 * and aggregating partial aggregator values from workers.
 *
 * Protocol:
 * 1. Before the beginning of superstep, worker receives its aggregators
 * from master, and these aggregators will be registered to this class.
 * Multiple registrations can be called concurrently.
 * 2. During the superstep, whenever a worker finishes computation,
 * it will send partial aggregated values to worker owner. This class is used
 * to help deserialize the arriving aggregator values, and aggregate the values
 * at the destination owner worker; these can happen concurrently.
 * (we know step 1. is finished before anything from step 2. happens because
 * other workers can't start computation before they receive aggregators
 * which this worker owns)
 * 3. This class also tracks the number of partial aggregator requests which
 * worker received. In the end of superstep, getMyAggregatorValuesWhenReady
 * will be called to ensure everything was received and get the values which
 * need to be sent to master.
 * Because of this counting, in step 2. even if worker owns no aggregators,
 * it will still send a message without aggregator data.
 * 4. In the end we reset to prepare for the next superstep.
 */
public class OwnerAggregatorServerData {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(OwnerAggregatorServerData.class);
  /** Map of aggregators which current worker owns */
  private final ConcurrentMap<String, Reducer<Object, Writable>>
  myReducerMap = Maps.newConcurrentMap();
  /**
   * Counts the requests with partial aggregated values from other workers.
   * It uses GlobalCommType.SPECIAL_COUNT to know how many requests it
   * has to receive.
   */
  private final TaskIdsPermitsBarrier workersBarrier;
  /** Progressable used to report progress */
  private final Progressable progressable;

  /**
   * Constructor
   *
   * @param progressable Progressable used to report progress
   */
  public OwnerAggregatorServerData(Progressable progressable) {
    this.progressable = progressable;
    workersBarrier = new TaskIdsPermitsBarrier(progressable);
  }

  /**
   * Register a reducer which current worker owns. Thread-safe.
   *
   * @param name Name of aggregator
   * @param reduceOp Reduce operation
   */
  public void registerReducer(String name,
      ReduceOperation<Object, Writable> reduceOp) {
    if (LOG.isDebugEnabled() && myReducerMap.isEmpty()) {
      LOG.debug("registerAggregator: The first registration after a reset()");
    }
    myReducerMap.putIfAbsent(name, new Reducer<>(reduceOp));
    progressable.progress();
  }

  /**
   * Reduce partial value of one of current worker's reducers.
   *
   * Thread-safe. Call only after reducers have been registered.
   *
   * @param name Name of the reducer
   * @param value Value to reduce to it
   */
  public void reduce(String name, Writable value) {
    Reducer<Object, Writable> reducer = myReducerMap.get(name);
    synchronized (reducer) {
      reducer.reduceMerge(value);
    }
    progressable.progress();
  }


  /**
   * Create initial value for a reducer. Used so requests
   * would be able to deserialize data.
   *
   * Thread-safe. Call only after reducer has been registered.
   *
   * @param name Name of the reducer
   * @return Empty value
   */
  public Writable createInitialValue(String name) {
    Reducer<Object, Writable> reducer = myReducerMap.get(name);
    synchronized (reducer) {
      return reducer.createInitialValue();
    }
  }

  /**
   * Notify this object that a partial aggregated values request from some
   * worker have been received. Thread-safe.
   */
  public void receivedRequestFromWorker() {
    workersBarrier.releaseOnePermit();
  }

  /**
   * Notify this object about the total number of requests which should
   * arrive from one of the workers. Thread-safe.
   *
   * @param requestCount Number of requests which should arrive
   * @param taskId Task id of that worker
   */
  public void receivedRequestCountFromWorker(long requestCount, int taskId) {
    workersBarrier.requirePermits(requestCount, taskId);
  }

  /**
   * This function will wait until all partial aggregated values from all
   * workers are ready and aggregated, and return final aggregated values
   * afterwards.
   *
   * @param workerIds All workers in the job apart from the current one
   * @return Iterable through final aggregated values which this worker owns
   */
  public Iterable<Map.Entry<String, Writable>>
  getMyReducedValuesWhenReady(Set<Integer> workerIds) {
    workersBarrier.waitForRequiredPermits(workerIds);
    if (LOG.isDebugEnabled()) {
      LOG.debug("getMyAggregatorValuesWhenReady: Values ready");
    }
    return Iterables.transform(myReducerMap.entrySet(),
        new Function<Map.Entry<String, Reducer<Object, Writable>>,
            Map.Entry<String, Writable>>() {
          @Override
          public Map.Entry<String, Writable> apply(
              Map.Entry<String, Reducer<Object, Writable>> aggregator) {
            return new AbstractMap.SimpleEntry<String, Writable>(
                aggregator.getKey(),
                aggregator.getValue().getCurrentValue());
          }
        });
  }

  /**
   * Prepare for next superstep
   */
  public void reset() {
    myReducerMap.clear();
    if (LOG.isDebugEnabled()) {
      LOG.debug("reset: Ready for next superstep");
    }
  }

}
