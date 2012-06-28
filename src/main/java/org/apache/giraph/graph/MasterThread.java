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

package org.apache.giraph.graph;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * Master thread that will coordinate the activities of the tasks.  It runs
 * on all task processes, however, will only execute its algorithm if it knows
 * it is the "leader" from ZooKeeper.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class MasterThread<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable> extends Thread {
  /** Counter group name for the Giraph timers */
  public static final String GIRAPH_TIMERS_COUNTER_GROUP_NAME = "Giraph Timers";
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(MasterThread.class);
  /** Reference to shared BspService */
  private CentralizedServiceMaster<I, V, E, M> bspServiceMaster = null;
  /** Context (for counters) */
  private final Context context;
  /** Use superstep counters? */
  private final boolean superstepCounterOn;
  /** Setup seconds */
  private double setupSecs = 0d;
  /** Superstep timer (in seconds) map */
  private final Map<Long, Double> superstepSecsMap =
      new TreeMap<Long, Double>();

  /**
   * Constructor.
   *
   * @param bspServiceMaster Master that already exists and setup() has
   *        been called.
   * @param context Context from the Mapper.
   */
  MasterThread(BspServiceMaster<I, V, E, M> bspServiceMaster,
      Context context) {
    super(MasterThread.class.getName());
    this.bspServiceMaster = bspServiceMaster;
    this.context = context;
    superstepCounterOn = context.getConfiguration().getBoolean(
        GiraphJob.USE_SUPERSTEP_COUNTERS,
        GiraphJob.USE_SUPERSTEP_COUNTERS_DEFAULT);
  }

  /**
   * The master algorithm.  The algorithm should be able to withstand
   * failures and resume as necessary since the master may switch during a
   * job.
   */
  @Override
  public void run() {
    // Algorithm:
    // 1. Become the master
    // 2. If desired, restart from a manual checkpoint
    // 3. Run all supersteps until complete
    try {
      long startMillis = System.currentTimeMillis();
      long endMillis = 0;
      bspServiceMaster.setup();
      if (bspServiceMaster.becomeMaster()) {
        // Attempt to create InputSplits if necessary. Bail out if that fails.
        if (bspServiceMaster.getRestartedSuperstep() !=
            BspService.UNSET_SUPERSTEP ||
            bspServiceMaster.createInputSplits() != -1) {
          long setupMillis = System.currentTimeMillis() - startMillis;
          context.getCounter(GIRAPH_TIMERS_COUNTER_GROUP_NAME,
              "Setup (milliseconds)").
              increment(setupMillis);
          setupSecs = setupMillis / 1000.0d;
          SuperstepState superstepState = SuperstepState.INITIAL;
          long cachedSuperstep = BspService.UNSET_SUPERSTEP;
          while (superstepState != SuperstepState.ALL_SUPERSTEPS_DONE) {
            long startSuperstepMillis = System.currentTimeMillis();
            cachedSuperstep = bspServiceMaster.getSuperstep();
            superstepState = bspServiceMaster.coordinateSuperstep();
            long superstepMillis = System.currentTimeMillis() -
                startSuperstepMillis;
            superstepSecsMap.put(Long.valueOf(cachedSuperstep),
                superstepMillis / 1000.0d);
            if (LOG.isInfoEnabled()) {
              LOG.info("masterThread: Coordination of superstep " +
                  cachedSuperstep + " took " +
                  superstepMillis / 1000.0d +
                  " seconds ended with state " + superstepState +
                  " and is now on superstep " +
                  bspServiceMaster.getSuperstep());
            }
            if (superstepCounterOn) {
              String counterPrefix;
              if (cachedSuperstep == -1) {
                counterPrefix = "Vertex input superstep";
              } else {
                counterPrefix = "Superstep " + cachedSuperstep;
              }
              context.getCounter(GIRAPH_TIMERS_COUNTER_GROUP_NAME,
                  counterPrefix +
                  " (milliseconds)").
                  increment(superstepMillis);
            }

            // If a worker failed, restart from a known good superstep
            if (superstepState == SuperstepState.WORKER_FAILURE) {
              bspServiceMaster.restartFromCheckpoint(
                  bspServiceMaster.getLastGoodCheckpoint());
            }
            endMillis = System.currentTimeMillis();
          }
          bspServiceMaster.setJobState(ApplicationState.FINISHED, -1, -1);
        }
      }
      bspServiceMaster.cleanup();
      if (!superstepSecsMap.isEmpty()) {
        context.getCounter(
            GIRAPH_TIMERS_COUNTER_GROUP_NAME,
            "Shutdown (milliseconds)").
            increment(System.currentTimeMillis() - endMillis);
        if (LOG.isInfoEnabled()) {
          LOG.info("setup: Took " + setupSecs + " seconds.");
        }
        for (Entry<Long, Double> entry : superstepSecsMap.entrySet()) {
          if (LOG.isInfoEnabled()) {
            if (entry.getKey().longValue() ==
                BspService.INPUT_SUPERSTEP) {
              LOG.info("vertex input superstep: Took " +
                  entry.getValue() + " seconds.");
            } else {
              LOG.info("superstep " + entry.getKey() + ": Took " +
                  entry.getValue() + " seconds.");
            }
          }
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("shutdown: Took " +
              (System.currentTimeMillis() - endMillis) /
              1000.0d + " seconds.");
          LOG.info("total: Took " +
              ((System.currentTimeMillis() - startMillis) /
              1000.0d) + " seconds.");
        }
        context.getCounter(
            GIRAPH_TIMERS_COUNTER_GROUP_NAME,
            "Total (milliseconds)").
            increment(System.currentTimeMillis() - startMillis);
      }
    } catch (IOException e) {
      LOG.error("masterThread: Master algorithm failed with " +
          "IOException ", e);
      throw new IllegalStateException(e);
    } catch (InterruptedException e) {
      LOG.error("masterThread: Master algorithm failed with " +
          "InterruptedException", e);
      throw new IllegalStateException(e);
    } catch (KeeperException e) {
      LOG.error("masterThread: Master algorithm failed with " +
          "KeeperException", e);
      throw new IllegalStateException(e);
    }
  }
}
