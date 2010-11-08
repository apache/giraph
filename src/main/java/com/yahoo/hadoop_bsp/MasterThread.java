package com.yahoo.hadoop_bsp;

import org.apache.zookeeper.KeeperException;
import org.json.JSONException;

/**
 * Master thread that will coordinate the activities of the tasks.  It runs
 * on all task processes, however, will only execute its algorithm if it knows
 * it is the "leader" from Zookeeper.
 * @author aching
 *
 */
public class MasterThread extends Thread {
	/** Reference to shared BspService */
	private BspService m_bspService = null;
	
	/** Constructor */
	MasterThread(BspService bspService) {
		m_bspService = bspService;
	}

	/**
	 * The master algorithm.  The algorithm should be able to withstand
	 * failures and resume as necessary since the master may switch during a 
	 * job.
	 */
	@Override
	public void run() {
		try {
			m_bspService.masterCreatePartitions();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
