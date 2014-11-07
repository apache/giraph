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

import java.util.List;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;

/**
 * Class for aggregator constants and utility methods
 */
public class AggregatorUtils {

  /** How big a single aggregator request can be (in bytes) */
  public static final String MAX_BYTES_PER_AGGREGATOR_REQUEST =
      "giraph.maxBytesPerAggregatorRequest";
  /** Default max size of single aggregator request (1MB) */
  public static final int MAX_BYTES_PER_AGGREGATOR_REQUEST_DEFAULT =
      1024 * 1024;
  /**
   * Whether or not to have a copy of aggregators for each compute thread.
   * Unless aggregators are very large and it would hurt the application to
   * have that many copies of them, user should use thread-local aggregators
   * to prevent synchronization when aggregate() is called (and get better
   * performance because of it).
   */
  public static final String USE_THREAD_LOCAL_AGGREGATORS =
      "giraph.useThreadLocalAggregators";
  /** Default is not to have a copy of aggregators for each thread */
  public static final boolean USE_THREAD_LOCAL_AGGREGATORS_DEFAULT = false;

  /** Do not instantiate */
  private AggregatorUtils() { }

  /**
   * Get owner of aggregator with selected name from the list of workers
   *
   * @param aggregatorName Name of the aggregators
   * @param workers List of workers
   * @return Worker which owns the aggregator
   */
  public static WorkerInfo getOwner(String aggregatorName,
      List<WorkerInfo> workers) {
    int index = Math.abs(aggregatorName.hashCode() % workers.size());
    return workers.get(index);
  }

  /**
   * Check if we should use thread local aggregators.
   *
   * @param conf Giraph configuration
   * @return True iff we should use thread local aggregators
   */
  public static boolean
  useThreadLocalAggregators(ImmutableClassesGiraphConfiguration conf) {
    return conf.getBoolean(USE_THREAD_LOCAL_AGGREGATORS,
        USE_THREAD_LOCAL_AGGREGATORS_DEFAULT);
  }

  /**
   * Get the warning message about usage of unregistered aggregator to be
   * printed to user. If user didn't register any aggregators also provide
   * the explanation on how to do so.
   *
   * @param aggregatorName The name of the aggregator which user tried to
   *                       access
   * @param hasRegisteredAggregators True iff user registered some aggregators
   * @param conf Giraph configuration
   * @return Warning message
   */
  public static String getUnregisteredAggregatorMessage(
      String aggregatorName, boolean hasRegisteredAggregators,
      ImmutableClassesGiraphConfiguration conf) {
    String message = "Tried to access aggregator which wasn't registered " +
        aggregatorName;
    if (!hasRegisteredAggregators) {
      message = message + "; Aggregators can be registered in " +
          "MasterCompute.initialize by calling " +
          "registerAggregator(aggregatorName, aggregatorClass). " +
          "Also be sure that you are correctly setting MasterCompute class, " +
          "currently using " + conf.getMasterComputeClass().getName();
    }
    return message;
  }

  /**
   * Get the warning message about usage of unregistered reducer to be
   * printed to user. If user didn't register any reducers also provide
   * the explanation on how to do so.
   *
   * @param reducerName The name of the aggregator which user tried to
   *                       access
   * @param hasRegisteredReducers True iff user registered some aggregators
   * @param conf Giraph configuration
   * @return Warning message
   */
  public static String getUnregisteredReducerMessage(
      String reducerName, boolean hasRegisteredReducers,
      ImmutableClassesGiraphConfiguration conf) {
    String message = "Tried to access reducer which wasn't registered " +
        reducerName;
    if (!hasRegisteredReducers) {
      message = message + "; Aggregators can be registered from " +
          "MasterCompute by calling registerReducer function. " +
          "Also be sure that you are correctly setting MasterCompute class, " +
          "currently using " + conf.getMasterComputeClass().getName();
    }
    return message;
  }

  /**
   * Get the warning message when user tries to access broadcast, without
   * previously setting it, to be printed to user.
   * If user didn't broadcast any value also provide
   * the explanation on how to do so.
   *
   * @param broadcastName The name of the broadcast which user tried to
   *                       access
   * @param hasBroadcasted True iff user has broadcasted value before
   * @param conf Giraph configuration
   * @return Warning message
   */
  public static String getUnregisteredBroadcastMessage(
      String broadcastName, boolean hasBroadcasted,
      ImmutableClassesGiraphConfiguration conf) {
    String message = "Tried to access broadcast which wasn't set before " +
        broadcastName;
    if (!hasBroadcasted) {
      message = message + "; Values can be broadcasted from " +
          "MasterCompute by calling broadcast function. " +
          "Also be sure that you are correctly setting MasterCompute class, " +
          "currently using " + conf.getMasterComputeClass().getName();
    }
    return message;
  }
}
