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

package org.apache.giraph.counters;

import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Hadoop Counters in group "Giraph Timers" for timing things.
 */
public class GiraphTimers extends HadoopCountersBase {
  /** Counter group name for the giraph timers */
  public static final String GROUP_NAME = "Giraph Timers";
  /** Counter name for setup msec */
  public static final String SETUP_MS_NAME = "Setup (ms)";
  /** Counter name for total msec */
  public static final String TOTAL_MS_NAME = "Total (ms)";
  /** Counter name for shutdown msec */
  public static final String SHUTDOWN_MS_NAME = "Shutdown (ms)";
  /** Counter name for initialize msec */
  public static final String INITIALIZE_MS_NAME = "Initialize (ms)";

  /** Singleton instance for everyone to use */
  private static GiraphTimers INSTANCE;

  /** Setup time in msec */
  private static final int SETUP_MS = 0;
  /** Total time in msec (doesn't include initialize time) */
  private static final int TOTAL_MS = 1;
  /** Shutdown time in msec */
  private static final int SHUTDOWN_MS = 2;
  /** Total time it takes to get minimum machines */
  private static final int INITIALIZE_MS = 3;
  /** How many whole job counters we have */
  private static final int NUM_COUNTERS = 4;

  /** superstep time in msec */
  private final Map<Long, GiraphHadoopCounter> superstepMsec;

  /** Whole job counters stored in this class */
  private final GiraphHadoopCounter[] jobCounters;

  /**
   * Internal use only. Create using Hadoop Context
   *
   * @param context Hadoop Context to use.
   */
  private GiraphTimers(Context context) {
    super(context, GROUP_NAME);
    jobCounters = new GiraphHadoopCounter[NUM_COUNTERS];
    jobCounters[SETUP_MS] = getCounter(SETUP_MS_NAME);
    jobCounters[TOTAL_MS] = getCounter(TOTAL_MS_NAME);
    jobCounters[SHUTDOWN_MS] = getCounter(SHUTDOWN_MS_NAME);
    jobCounters[INITIALIZE_MS] = getCounter(INITIALIZE_MS_NAME);
    superstepMsec = Maps.newHashMap();
  }

  /**
   * Instantiate with Hadoop Context.
   *
   * @param context Hadoop Context to use.
   */
  public static void init(Context context) {
    INSTANCE = new GiraphTimers(context);
  }

  /**
   * Get singleton instance.
   *
   * @return singleton GiraphTimers instance.
   */
  public static GiraphTimers getInstance() {
    return INSTANCE;
  }

  /**
   * Get counter for setup time in milliseconds
   *
   * @return Counter for setup time in milliseconds
   */
  public GiraphHadoopCounter getSetupMs() {
    return jobCounters[SETUP_MS];
  }

  /**
   * Get counter for superstep time in milliseconds
   *
   * @param superstep Integer superstep number.
   * @param computationName Name of the computation for display (may be null)
   * @return Counter for setup time in milliseconds
   */
  public GiraphHadoopCounter getSuperstepMs(long superstep,
                                            String computationName) {
    GiraphHadoopCounter counter = superstepMsec.get(superstep);
    if (counter == null) {
      String counterPrefix;
      if (superstep == -1) {
        counterPrefix = "Input superstep";
      } else {
        counterPrefix = "Superstep " + superstep +
            (computationName == null ? "" : " " + computationName);
      }
      counter = getCounter(counterPrefix + " (ms)");
      superstepMsec.put(superstep, counter);
    }
    return counter;
  }

  /**
   * Get counter for total time in milliseconds (doesn't include initialize
   * time).
   *
   * @return Counter for total time in milliseconds.
   */
  public GiraphHadoopCounter getTotalMs() {
    return jobCounters[TOTAL_MS];
  }

  /**
   * Get counter for shutdown time in milliseconds.
   *
   * @return Counter for shutdown time in milliseconds.
   */
  public GiraphHadoopCounter getShutdownMs() {
    return jobCounters[SHUTDOWN_MS];
  }

  /**
   * Get counter for initializing the process,
   * having to wait for a minimum number of processes to be available
   * before setup step
   * @return Counter for initializing in milliseconds
   */
  public GiraphHadoopCounter getInitializeMs() {
    return jobCounters[INITIALIZE_MS];
  }

  /**
   * Get map of superstep to msec counter.
   *
   * @return mapping of superstep to msec counter.
   */
  public Map<Long, GiraphHadoopCounter> superstepCounters() {
    return superstepMsec;
  }

  /**
   * Get Iterable through job counters.
   *
   * @return Iterable of job counters.
   */
  public Iterable<GiraphHadoopCounter> jobCounters() {
    return Arrays.asList(jobCounters);
  }

  @Override
  public Iterator<GiraphHadoopCounter> iterator() {
    return Iterators.concat(jobCounters().iterator(),
        superstepCounters().values().iterator());
  }
}
