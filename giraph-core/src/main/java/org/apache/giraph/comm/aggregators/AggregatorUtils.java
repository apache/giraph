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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;

import java.util.List;

/**
 * Class for aggregator constants and utility methods
 */
public class AggregatorUtils {
  /**
   * Special aggregator name which will be used to send the total number of
   * aggregators requests which should arrive
   */
  public static final String SPECIAL_COUNT_AGGREGATOR =
      "__aggregatorRequestCount";
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
   * Get aggregator class from class name, catch all exceptions.
   *
   * @param aggregatorClassName Class nam of aggregator class
   * @return Aggregator class
   */
  public static Class<Aggregator<Writable>> getAggregatorClass(String
      aggregatorClassName) {
    try {
      return (Class<Aggregator<Writable>>) Class.forName(aggregatorClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("getAggregatorClass: " +
          "ClassNotFoundException for aggregator class " + aggregatorClassName,
          e);
    }
  }

  /**
   * Create new aggregator instance from aggregator class,
   * catch all exceptions.
   *
   * @param aggregatorClass Class of aggregator
   * @param conf Configuration
   * @return New aggregator
   */
  public static Aggregator<Writable> newAggregatorInstance(
      Class<Aggregator<Writable>> aggregatorClass,
      ImmutableClassesGiraphConfiguration conf) {
    return ReflectionUtils.newInstance(aggregatorClass, conf);
  }

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
}
