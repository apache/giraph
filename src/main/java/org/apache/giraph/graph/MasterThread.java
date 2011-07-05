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

import org.apache.giraph.bsp.CentralizedServiceMaster;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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

    /**
     *  Constructor.
     *
     *  @param bspServiceMaster Master that already exists and setup() has
     *         been called.
     */
    MasterThread(BspServiceMaster<I, V, E, M> bspServiceMaster) {
        super(MasterThread.class.getName());
        this.bspServiceMaster = bspServiceMaster;
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
            bspServiceMaster.setup();
            if (bspServiceMaster.becomeMaster() == true) {
                if (bspServiceMaster.getRestartedSuperstep() == -1) {
                    bspServiceMaster.createInputSplits();
                }
                CentralizedServiceMaster.SuperstepState superstepState =
                    CentralizedServiceMaster.SuperstepState.INITIAL;
                long cachedSuperstep = -1;
                while (superstepState !=
                        CentralizedServiceMaster.SuperstepState.
                        ALL_SUPERSTEPS_DONE) {
                    cachedSuperstep = bspServiceMaster.getSuperstep();
                    superstepState = bspServiceMaster.coordinateSuperstep();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("masterThread: Coordination of superstep " +
                                 cachedSuperstep +
                                 " ended with state " + superstepState +
                                 " and is now on superstep " +
                                 bspServiceMaster.getSuperstep());
                    }

                    // If a worker failed, restart from a known good superstep
                    if (superstepState ==
                            CentralizedServiceMaster.
                                SuperstepState.WORKER_FAILURE) {
                        bspServiceMaster.restartFromCheckpoint(
                            bspServiceMaster.getLastGoodCheckpoint());
                    }

                }
                bspServiceMaster.setJobState(BspService.State.FINISHED,
                                               -1,
                                               -1);
            }
            bspServiceMaster.cleanup();
        } catch (Exception e) {
            LOG.error("masterThread: Master algorithm failed: ", e);
            throw new RuntimeException(e);
        }
    }
}
