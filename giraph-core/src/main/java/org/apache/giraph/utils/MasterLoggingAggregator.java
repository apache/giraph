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

package org.apache.giraph.utils;

import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.master.MasterAggregatorUsage;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Helper class for using aggregator which gathers log messages from workers
 * and prints them on master.
 *
 * If you want to track what's going on in your application,
 * and want to have all those logs accessible in a single place in the end of
 * each superstep, you can use option from this class.
 *
 * If you use a lot of log messages this might slow down your application,
 * but it can easily be turned on/off without changing your code just by
 * switching the option.
 */
public class MasterLoggingAggregator {
  /** Whether or not to use master logging aggregator */
  public static final String USE_MASTER_LOGGING_AGGREGATOR =
      "giraph.useMasterLoggingAggregator";
  /** Default is not using master logging aggregator */
  public static final boolean USE_MASTER_LOGGING_AGGREGATOR_DEFAULT = false;
  /** Name of aggregator which will be gathering the logs */
  public static final String MASTER_LOGGING_AGGREGATOR_NAME =
      "masterLoggingAggregator";

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MasterLoggingAggregator.class);

  /** Do not instantiate */
  private MasterLoggingAggregator() {
  }

  /**
   * Check if master logging aggregator is used.
   *
   * @param conf Configuration
   * @return True iff master logging aggregator is used
   */
  public static boolean useMasterLoggingAggregator(Configuration conf) {
    return conf.getBoolean(USE_MASTER_LOGGING_AGGREGATOR,
        USE_MASTER_LOGGING_AGGREGATOR_DEFAULT);
  }

  /**
   * Set whether or not master logging aggregator should be used
   *
   * @param useMasterLoggingAggregator Whether or not we want
   *                                   master logging aggregator to be used
   * @param conf                       Configuration
   */
  public static void setUseMasterLoggingAggregator(
      boolean useMasterLoggingAggregator, Configuration conf) {
    conf.setBoolean(USE_MASTER_LOGGING_AGGREGATOR, useMasterLoggingAggregator);
  }

  /**
   * Aggregate some message to master logging aggregator,
   * if the option for using it is set in the configuration.
   *
   * This is the method application implementation should use
   * in order to add message to the aggregator.
   *
   * @param message               Message to log
   * @param workerAggregatorUsage Worker aggregator usage
   *                              (can be Vertex, WorkerContext, etc)
   * @param conf                  Configuration
   */
  public static void aggregate(String message,
      WorkerAggregatorUsage workerAggregatorUsage, Configuration conf) {
    if (useMasterLoggingAggregator(conf)) {
      workerAggregatorUsage.aggregate(
          MASTER_LOGGING_AGGREGATOR_NAME, new Text(message));
    }
  }

  /**
   * Register master logging aggregator,
   * if the option for using it is set in the configuration.
   *
   * This method will be called by Giraph infrastructure on master.
   *
   * @param masterAggregatorUsage Master aggregator usage
   * @param conf                  Configuration
   */
  public static void registerAggregator(
      MasterAggregatorUsage masterAggregatorUsage, Configuration conf) {
    if (useMasterLoggingAggregator(conf)) {
      try {
        masterAggregatorUsage.registerAggregator(MASTER_LOGGING_AGGREGATOR_NAME,
            TextAppendAggregator.class);
      } catch (InstantiationException e) {
        throw new IllegalStateException("registerAggregator: " +
            "InstantiationException occurred");
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("registerAggregator: " +
            "IllegalAccessException occurred");
      }
    }
  }

  /**
   * Print value of master logging aggregator on the master log,
   * if the option for using it is set in the configuration.
   *
   * This method will be called by Giraph infrastructure on master.
   *
   * @param masterAggregatorUsage Master aggregator usage
   * @param conf                  Configuration
   */
  public static void logAggregatedValue(
      MasterAggregatorUsage masterAggregatorUsage, Configuration conf) {
    if (useMasterLoggingAggregator(conf) && LOG.isInfoEnabled()) {
      LOG.info("logAggregatedValue: \n" +
          masterAggregatorUsage.getAggregatedValue(
              MASTER_LOGGING_AGGREGATOR_NAME));
    }
  }
}
