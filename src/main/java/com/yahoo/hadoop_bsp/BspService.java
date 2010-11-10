package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.net.InetAddress;

import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.log.Log;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 * @author aching
 *
 */
public class BspService implements CentralizedService, Watcher {
	/** Private Zookeeper instance that implements the service */
	private ZooKeeperExt m_zk = null;
	/** My virtual identity in the group */
	private int m_myVirtualId = -1;
	/** Am I the master? */
	private boolean m_isMaster = false;
	/** All barriers are synchronized with this object */
	private Object m_barrierObject;
	/** Registration synchronization */
	private BspEvent m_partitionCountSet = new PredicateLock();
	/** Partition count */
	private Integer m_partitionCount;
	/** Configuration of the job*/
	private Configuration m_conf;
	/** Current superstep */
	Integer m_currentSuperStep = -1;
	/** Job id, to ensure uniqueness */
	String m_jobId;
	/** Task id, to ensure uniqueness */
	String m_taskId;
	/** My process health znode */
	String m_myHealthZode;
	/** Master thread */
	Thread m_masterThread;
	/** Class logger */
    private static final Logger LOG = Logger.getLogger(BspService.class);
    /** State of the service? */
    public enum State {
    	INIT, 
    	RUNNING, 
    	FAILED, 
    	FINISHED
    }
    /** Current state */
    private State m_currentState = State.INIT;
        
	public static final String BASE_DIR = "/_hadoopBsp";
	public static final String BARRIER_DIR = "/_barrier";
	public static final String SUPERSTEP_NODE = "/_superstep";
	public static final String PROCESS_HEALTH_DIR = "/_processHealth";
	public static final String PARTITION_COUNT_NODE = "/_partitionCount";
	public static final String VIRTUAL_ID_DIR = "/_virtualIdDir";
	public static final String JOB_STATE_NODE = "/_jobState";

	private final String BASE_PATH;
	private final String BARRIER_PATH;
	private final String SUPERSTEP_PATH;
	private final String PROCESS_HEALTH_PATH;
	private final String PARTITION_COUNT_PATH;
	private final String VIRTUAL_ID_PATH;
	private final String JOB_STATE_PATH;
	
	public BspService(String serverPortList, int sessionMsecTimeout, 
		Configuration conf) throws IOException, KeeperException, InterruptedException, 
		JSONException {
		m_conf = conf;
		m_jobId = conf.get("mapred.job.id", "Unknown Job");
		m_taskId = conf.get("mapred.task.id", "Unknown Task");

		BASE_PATH = "/" + m_jobId + BASE_DIR;
		BARRIER_PATH = BASE_PATH + BARRIER_DIR;
		SUPERSTEP_PATH = BASE_PATH + SUPERSTEP_NODE;
		PROCESS_HEALTH_PATH = BASE_PATH + PROCESS_HEALTH_DIR;
		PARTITION_COUNT_PATH = BASE_PATH + PARTITION_COUNT_NODE;
		VIRTUAL_ID_PATH = BASE_PATH + VIRTUAL_ID_DIR;
		JOB_STATE_PATH = BASE_PATH + JOB_STATE_NODE;
			
	    m_zk = new ZooKeeperExt(serverPortList, sessionMsecTimeout, this);
	    m_masterThread = new MasterThread(this);
	    m_masterThread.start();
	}

	/** 
	 * Intended to check the health of the node.  For instance, can it ssh, 
	 * dmesg, etc. 
	 */
	public boolean isHealthy() {
		return true;
	}
	
	/**
	 * If the master decides that this job doesn't have the resources to 
	 * continue, it can fail the job.
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public synchronized void masterSetJobState(State state) 
		throws KeeperException, InterruptedException {
		m_currentState = state;
		try {
			m_zk.createExt(JOB_STATE_PATH, 
						   state.toString().getBytes(),
						   Ids.OPEN_ACL_UNSAFE, 
						   CreateMode.PERSISTENT,
						   true);
		} catch (KeeperException.NodeExistsException e) {
			m_zk.setData(JOB_STATE_PATH, state.toString().getBytes(), -1);
		}
	}
	
	public synchronized State getJobState() {
		return m_currentState;
	}
	
	/**
	 * Only the 'master' should be doing this.  Wait until the number of
	 * processes that have reported health exceeds the minimum percentage.
	 * If the minimum percentage is not met, fail the job.  Otherwise, create 
	 * the virtual process ids, assign them to the processes, and then create
	 * the PARTITION_COUNT znode. 
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws JSONException 
	 */
	public void masterCreatePartitions() 
		throws KeeperException, InterruptedException, JSONException {
		try {
			if (m_zk.exists(PARTITION_COUNT_PATH, false) != null) {
				LOG.info(PARTITION_COUNT_PATH + 
						 " already exists, no need to create");
				return;
			}
		} catch (KeeperException.NoNodeException e) {
			LOG.info("Need to create the partitions at " + 
					 PARTITION_COUNT_PATH);
		}
		int maxPollAttempts = m_conf.getInt(BspJob.BSP_POLL_ATTEMPTS, 
											BspJob.DEFAULT_BSP_POLL_ATTEMPTS);
		int initialProcs = m_conf.getInt(BspJob.BSP_INITIAL_PROCESSES, -1);
		int minProcs = m_conf.getInt(BspJob.BSP_MIN_PROCESSES, -1);
		float minPercentResponded = 
			m_conf.getFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 0.0f);
		List<String> procsReported = null;
		int msecsPollPeriod = m_conf.getInt(BspJob.BSP_POLL_MSECS, 
											BspJob.DEFAULT_BSP_POLL_MSECS);
		boolean failJob = true;
		int pollAttempt = 0;
		while (pollAttempt < maxPollAttempts) {
			Thread.sleep(msecsPollPeriod);
			LOG.info("Sleeping for " + msecsPollPeriod + " msecs for " +
					 maxPollAttempts + " attempts.");
			try {
				procsReported = m_zk.getChildren(PROCESS_HEALTH_PATH, false);
				if ((procsReported.size() * 100.0f / initialProcs) >= 
					minPercentResponded) {
					failJob = false;
					break;
				}
			} catch (KeeperException.NoNodeException e) {
				LOG.info("No node " + PROCESS_HEALTH_PATH + " exists: " + 
						 e.getMessage());
			}
			++pollAttempt;
		}
		if (failJob) {
			masterSetJobState(State.FAILED);
			throw new InterruptedException(
			    "Did not receive enough processes in time (only " + 
			    procsReported.size());
		}
		
		int healthyProcs = 0;
		for (String proc : procsReported) {
			try {
				String jsonObject = 
					new String(m_zk.getData(PROCESS_HEALTH_PATH + "/" + proc, 
							   false, 
							   null));
				LOG.info("Health of " + proc + " = " + jsonObject);
				Boolean processHealth = 
					(Boolean) JSONObject.stringToValue(jsonObject);
				if (processHealth.booleanValue()) {
					++healthyProcs;
				}
			} catch (KeeperException.NoNodeException e) {
				LOG.error("Process at " + proc + 
						  " between retrieving children and getting znode");
			}
		}
		if (healthyProcs < minProcs) {
			masterSetJobState(State.FAILED);
			throw new InterruptedException(
				"Only " + Integer.toString(healthyProcs) + " available when " + 
				Integer.toString(minProcs) + " are required.");
		}

		/*
		 *  When creating znodes, in case the master has already run, resume 
		 *  where it left off.
		 */
		try {
			m_zk.create(VIRTUAL_ID_PATH, 
						null,
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("Node " + VIRTUAL_ID_PATH + " already exists.");
		}
		for (int i = 0; i < healthyProcs; ++i) {
			try {
				m_zk.create(VIRTUAL_ID_PATH + "/" + Integer.toString(i), 
							null,
					  		Ids.OPEN_ACL_UNSAFE, 
					  		CreateMode.PERSISTENT);
				LOG.info("Created virtual id " + VIRTUAL_ID_PATH + "/" + 
						 Integer.toString(i));
			} catch (KeeperException.NodeExistsException e) {
					LOG.info("Node " + VIRTUAL_ID_PATH+ "/" + 
							Integer.toString(i) +" already exists.");
			}
		}
		try {
			m_zk.create(SUPERSTEP_PATH,
						Integer.toString(0).getBytes(),
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("Node " + SUPERSTEP_PATH + " already exists.");
		}
		try {
			m_zk.create(PARTITION_COUNT_PATH, 
						Integer.toString(healthyProcs).getBytes(),
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
			LOG.info("Created partition count path " + PARTITION_COUNT_PATH + 
					 " with count = " + healthyProcs);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("Node " + PARTITION_COUNT_PATH + " already exists.");	
		}
	}
	
	public void setup() {
		/*
		 * Determine the virtual id of every process.
		 * *
		 * 1) Everyone creates their health node
		 * 2) If PARTITION_COUNT exists, goto 5)
		 * 3) Wait for on PARTITION_COUNT node to be created
		 * 5) Everyone checks their own node to see the virtual id "suggested"
		 *    by the master. (Future work)
		 * 6) Try the suggested virtual id, otherwise, scan for a free one.
		 */
		try {
			m_myHealthZode =
				m_zk.createExt(
					PROCESS_HEALTH_PATH + "/" 
					+ InetAddress.getLocalHost().getHostName() + "-" + m_taskId, 
					Boolean.toString(isHealthy()).getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL,
					true);
			LOG.info("Created my health node: " + m_myHealthZode);
			byte[] partitionCountByteArr = null;
			try {
				m_zk.exists(PARTITION_COUNT_PATH, true);
				partitionCountByteArr = 
					m_zk.getData(PARTITION_COUNT_PATH, true, null);
			}
			catch (KeeperException.NoNodeException e) {
				m_partitionCountSet.waitForever();
				partitionCountByteArr = 
					m_zk.getData(PARTITION_COUNT_PATH, true, null);
			}
			finally {
				m_partitionCount = (Integer) 
					JSONObject.stringToValue(new String(partitionCountByteArr));
			}
			
		    m_currentSuperStep = (Integer) JSONObject.stringToValue(
			    	new String(m_zk.getData(SUPERSTEP_PATH, false, null)));
		    LOG.info("Using super step " + m_currentSuperStep);
		    
		    /*
		     *  Try to claim a virtual id in a polling period.
		     */
		    List<String> virtualIdList = null;
		    boolean reservedNode = false;
		    while ((getJobState() != State.FAILED) &&
		    	   (getJobState() != State.FINISHED)) {
		    	virtualIdList = m_zk.getChildren(VIRTUAL_ID_PATH, false);
		    	for (String virtualId : virtualIdList) {
		    		try {
		    			m_zk.create(virtualId + "/reserved", 
		    					    null,
		    					    Ids.OPEN_ACL_UNSAFE, 
		    					    CreateMode.EPHEMERAL);
			    		LOG.info("Reserved virtual id " + virtualId);
		    			reservedNode = true;
		    			break;
		    		} catch (KeeperException.NodeExistsException e) {
		    			LOG.info("Failed to reserve " + virtualId);
		    		}
		    	}
		    	if (reservedNode) {
		    		return;
		    	}
		    	Thread.sleep(
		    		m_conf.getInt(BspJob.BSP_POLL_MSECS, 
		    				      BspJob.DEFAULT_BSP_POLL_MSECS));
			} 
		} catch (Exception e) {
			LOG.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}
	
	public void barrier() {
		/* Note that this barrier blocks until success.  It would be best if 
		 * it were interruptable if for instance there was a failure. */
		
	}

	public InputSplit getInputSplit() {
		// TODO Auto-generated method stub
		return null;
	}

	public void process(WatchedEvent event) {
		LOG.info("Got a new event: path = " + event.getPath() + ", type = "
				 + event.getType() + ", state = " + event.getState());
		/* Nothing to do */
		if (event.getPath() == null) {
			return;
		}
		
		if (event.getPath().contains(BARRIER_DIR)) {
			synchronized (m_barrierObject) {
				m_barrierObject.notifyAll();				
			}
		}
		else if (event.getPath().equals(PARTITION_COUNT_PATH)) {
			m_partitionCountSet.signal();
		}
		else if (event.getPath().equals(JOB_STATE_PATH)) {
			try {
				synchronized (this) {
					m_currentState = State.valueOf(
							new String(m_zk.getData(event.getPath(), true, null)));					
				}
			} catch (KeeperException.NoNodeException e) {
				LOG.error(JOB_STATE_PATH + " was unusually removed.");
			} catch (Exception e) {
				// Shouldn't ever happen
				m_currentState = State.FAILED;
			}
		}
	}
	
	public int getSuperStep() {
		return m_currentSuperStep;
	}
	
	public void cleanup() {
		try {
			m_masterThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException("Master thread couldn't join");
		}
		try {
			m_zk.close();
		} catch (InterruptedException e) {
			throw new RuntimeException("Zookeeper failed to close");
		}
	}
}
