/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 * Master thread that will coordinate the activities of the tasks.  It runs
 * on all task processes, however, will only execute its algorithm if it knows
 * it is the "leader" from ZooKeeper.
 */
@SuppressWarnings("rawtypes")
public class MasterThread<I extends WritableComparable,
                          V extends Writable,
                          E extends Writable,
                          M extends Writable> extends Thread {
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
        new HashMap<Long, Double>();
    /**
     *  Constructor.
     *
     *  @param bspServiceMaster Master that already exists and setup() has
     *         been called.
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
            if (bspServiceMaster.becomeMaster() == true) {
                if (bspServiceMaster.getRestartedSuperstep() == -1) {
                    bspServiceMaster.createInputSplits();
                }
                long setupMillis = (System.currentTimeMillis() - startMillis);
                context.getCounter("Giraph Timers", "Setup (milliseconds)").
                    increment(setupMillis);
                setupSecs = setupMillis / 1000.0d;
                CentralizedServiceMaster.SuperstepState superstepState =
                    CentralizedServiceMaster.SuperstepState.INITIAL;
                long cachedSuperstep = -1;
                while (superstepState !=
                        CentralizedServiceMaster.SuperstepState.
                        ALL_SUPERSTEPS_DONE) {
                    long startSuperstepMillis = System.currentTimeMillis();
                    cachedSuperstep = bspServiceMaster.getSuperstep();
                    superstepState = bspServiceMaster.coordinateSuperstep();
                    long superstepMillis = System.currentTimeMillis() -
                        startSuperstepMillis;
                    superstepSecsMap.put(new Long(cachedSuperstep),
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
                        context.getCounter("Giraph Timers", "Superstep " +
                                           cachedSuperstep + " (milliseconds)").
                                           increment(superstepMillis);
                    }

                    // If a worker failed, restart from a known good superstep
                    if (superstepState ==
                            CentralizedServiceMaster.
                                SuperstepState.WORKER_FAILURE) {
                        bspServiceMaster.restartFromCheckpoint(
                            bspServiceMaster.getLastGoodCheckpoint());
                    }
                    endMillis = System.currentTimeMillis();
                }
                bspServiceMaster.setJobState(BspService.State.FINISHED,
                                             -1,
                                             -1);
            }
            bspServiceMaster.cleanup();
            if (!superstepSecsMap.isEmpty()) {
                context.getCounter("Giraph Timers", "Shutdown (milliseconds)").
                    increment(System.currentTimeMillis() - endMillis);
                if (LOG.isInfoEnabled()) {
                    LOG.info("setup: Took " + setupSecs + " seconds.");
                }
                for (Entry<Long, Double> entry : superstepSecsMap.entrySet()) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("superstep " + entry.getKey() + ": Took " +
                                 entry.getValue() + " seconds.");
                    }
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("shutdown: Took " +
                             (System.currentTimeMillis() - endMillis) /
                             1000.0d + " seconds.");
                    LOG.info("total: Took " +
                             ((System.currentTimeMillis() / 1000.0d) -
                             setupSecs) + " seconds.");
                }
                context.getCounter("Giraph Timers", "Total (milliseconds)").
                    increment(System.currentTimeMillis() - startMillis);
            }
        } catch (Exception e) {
            LOG.error("masterThread: Master algorithm failed: ", e);
            throw new RuntimeException(e);
        }
    }
}
