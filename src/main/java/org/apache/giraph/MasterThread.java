package org.apache.giraph;

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
    private CentralizedServiceMaster<I, V, E, M> m_bspServiceMaster = null;

    /**
     *  Constructor.
     *
     *  @param bspServiceMaster Master that already exists and setup() has
     *         been called.
     */
    MasterThread(BspServiceMaster<I, V, E, M> bspServiceMaster) {
        super(MasterThread.class.getName());
        m_bspServiceMaster = bspServiceMaster;
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
            m_bspServiceMaster.setup();
            if (m_bspServiceMaster.becomeMaster() == true) {
                if (m_bspServiceMaster.getRestartedSuperstep() == -1) {
                    m_bspServiceMaster.createInputSplits();
                }
                CentralizedServiceMaster.SuperstepState superstepState =
                    CentralizedServiceMaster.SuperstepState.INITIAL;
                long cachedSuperstep = -1;
                while (superstepState !=
                        CentralizedServiceMaster.SuperstepState.
                        ALL_SUPERSTEPS_DONE) {
                    cachedSuperstep = m_bspServiceMaster.getSuperstep();
                    superstepState = m_bspServiceMaster.coordinateSuperstep();
                    if (LOG.isInfoEnabled()) {
                        LOG.info("masterThread: Coordination of superstep " +
                                 cachedSuperstep +
                                 " ended with state " + superstepState +
                                 " and is now on superstep " +
                                 m_bspServiceMaster.getSuperstep());
                    }

                    // If a worker failed, restart from a known good superstep
                    if (superstepState ==
                            CentralizedServiceMaster.
                                SuperstepState.WORKER_FAILURE) {
                        m_bspServiceMaster.restartFromCheckpoint(
                            m_bspServiceMaster.getLastGoodCheckpoint());
                    }

                }
                m_bspServiceMaster.setJobState(BspService.State.FINISHED,
                                               -1,
                                               -1);
            }
            m_bspServiceMaster.cleanup();
        } catch (Exception e) {
            LOG.error("masterThread: Master algorithm failed: ", e);
            throw new RuntimeException(e);
        }
    }
}
