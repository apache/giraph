package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Master thread that will coordinate the activities of the tasks.  It runs
 * on all task processes, however, will only execute its algorithm if it knows
 * it is the "leader" from ZooKeeper.
 * @author aching
 *
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
        try {
            m_bspServiceMaster.setup();
            if (m_bspServiceMaster.becomeMaster() == true) {
                if (m_bspServiceMaster.getManualRestartSuperstep() == -1) {
                    m_bspServiceMaster.createInputSplits();
                }
                // TODO: When one becomes a master, might need to "watch" the
                // selected workers of the current superstep to insure they
                // are alive.
                while (!m_bspServiceMaster.coordinateSuperstep()) {
                    LOG.info("masterThread: Finished superstep " +
                             (m_bspServiceMaster.getSuperstep() - 1));
                }
                m_bspServiceMaster.setJobState(BspService.State.FINISHED);
            }
            m_bspServiceMaster.cleanup();
        } catch (Exception e) {
            LOG.error("masterThread: Master algorithm failed: ", e);
            throw new RuntimeException(e);
        }
    }
}
