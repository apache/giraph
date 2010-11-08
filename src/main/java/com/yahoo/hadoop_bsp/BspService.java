package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
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
	private ZooKeeper m_zk = null;
	/** My virtual identity in the group */
	private int m_myVirtualId = -1;
	/** Am I the master? */
	private boolean m_isMaster = false;
	/** All barriers are synchronized with this object */
	private Object m_barrierObject;
	/** Registration synchronization */
	private BspEvent m_partitionCountSet = new PredicateLock();
	private Boolean m_partitionCountReady = false;
	/** Configuration of the job*/
	private Configuration m_conf;
	Integer m_currentSuperStep = -1;
	/** Job id, to ensure uniqueness */
	String m_jobId;
	/** Master thread */
	Thread m_masterThread;
	/** Class logger */
    private static final Logger LOG = Logger.getLogger(BspService.class);

	public static final String BASE_DIR = "/_hadoopBsp";
	public static final String BARRIER_DIR = "/_barrier";
	public static final String SUPERSTEP_DIR = "/_superstep";
	public static final String PROCESS_HEALTH_DIR = "/_processHealth";
	public static final String PARTITION_COUNT = "/_partitionCount";
	public static final String VIRTUAL_ID_DIR = "/_virtualIdDir";
	public static final String FAIL_JOB_DIR = "/_failJob";

	private final String BASE_PATH;
	private final String BARRIER_PATH;
	private final String SUPERSTEP_PATH;
	private final String PROCESS_HEALTH_PATH;
	private final String PARTITION_COUNT_PATH;
	private final String VIRTUAL_ID_PATH;
	private final String FAIL_JOB_PATH;
	
	public BspService(String serverPortList, int sessionMsecTimeout, 
		Configuration conf) throws IOException, KeeperException, InterruptedException, 
		JSONException {
		m_conf = conf;
		m_jobId = conf.get("mapred.job.id", "Unknown Job");
		BASE_PATH = "/" + m_jobId + BASE_DIR;
		BARRIER_PATH = BASE_PATH + BARRIER_DIR;
		SUPERSTEP_PATH = BASE_PATH + SUPERSTEP_DIR;
		PROCESS_HEALTH_PATH= BASE_PATH + PROCESS_HEALTH_DIR;
		PARTITION_COUNT_PATH = BASE_PATH + PARTITION_COUNT;
		VIRTUAL_ID_PATH = BASE_PATH + VIRTUAL_ID_DIR;
		FAIL_JOB_PATH = BASE_PATH + FAIL_JOB_DIR;
			
	    m_zk = new ZooKeeper(serverPortList, sessionMsecTimeout, this);
	    m_currentSuperStep = (Integer) JSONObject.stringToValue(
	    	new String(m_zk.getData(SUPERSTEP_DIR, false, null)));
	    
	    m_masterThread = new MasterThread(this);
	    m_masterThread.start();
	}

	public boolean isHealthy() {
		return false;
	}
	
	/**
	 * If the master decides that this job doesn't have the resources to 
	 * continue, it can fail the job.
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public void masterFailJob() throws KeeperException, InterruptedException {
		m_zk.create(FAIL_JOB_PATH, 
				    null,
				    Ids.OPEN_ACL_UNSAFE, 
			   	    CreateMode.PERSISTENT);
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
		int pollAttempt = 0;
		int maxPollAttempts = m_conf.getInt(BspJob.BSP_POLL_ATTEMPTS, 
											BspJob.DEFAULT_BSP_POLL_ATTEMPTS);
		int initialProcs = m_conf.getInt(BspJob.BSP_INITIAL_PROCESSES, -1);
		int minProcs = m_conf.getInt(BspJob.BSP_MIN_PROCESSES, -1);
		float minPercentResponded = 
			m_conf.getFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 0.0f);
		List<String> procsReported = null;
		boolean failJob = true;
		while (pollAttempt < maxPollAttempts) {
			procsReported = m_zk.getChildren(PROCESS_HEALTH_PATH, false);
			if (procsReported.size() / initialProcs >= minPercentResponded) {
				failJob = false;
				break;
			}
		}
		if (failJob) {
			masterFailJob();
			throw new InterruptedException(
			    "Did not receive enough processes in time");
		}
		
		int healthyProcs = 0;
		Iterator procsReportedIt = procsReported.iterator();
		while (procsReportedIt.hasNext()) {
			try {
				String jsonObject = new String(
					m_zk.getData((String) procsReportedIt.next(), false, null));
				Boolean processHealth = 
					(Boolean) JSONObject.stringToValue(jsonObject);
				if (processHealth.booleanValue()) {
					++healthyProcs;
				}
			}
			catch (KeeperException e){
				LOG.error("Process at " + procsReportedIt.next() + 
						  " between retrieving children and getting znode");
			}
		}
		if (healthyProcs < minProcs) {
			masterFailJob();
			throw new InterruptedException(
				"Only " + Integer.toString(healthyProcs) + " available when " + 
				Integer.toString(minProcs) + " are required.");
		}
		
		m_zk.create(BASE_DIR + VIRTUAL_ID_DIR, 
				  null,
				  Ids.OPEN_ACL_UNSAFE, 
				  CreateMode.PERSISTENT);
		for (int i = 0; i < healthyProcs; ++i) {
			m_zk.create(BASE_DIR + VIRTUAL_ID_DIR + "/" + Integer.toString(i), 
					  null,
					  Ids.OPEN_ACL_UNSAFE, 
					  CreateMode.PERSISTENT);
		}
		m_zk.create(BASE_DIR + PARTITION_COUNT, 
				  Integer.toString(healthyProcs).getBytes(),
				  Ids.OPEN_ACL_UNSAFE, 
				  CreateMode.PERSISTENT);
	}
	
	
	public void setup() {
		/*
		 * Determine the virtual id of every process.
		 * *
		 * 1) Everyone creates their health node
		 * 2) If PARTITION_COUNT exists, goto 5)
		 * 2) Wait for event on the node below to see if they are the master.
		 * 3) Wait for exist event on PARTITION_COUNT node
		 * 4) Master - lowest child, determines whether it is sufficient to 
		 *    proceed based on the minimum required processes, the min % 
		 *    responded, a polling timeout, and a maximum timeout.  Write out
		 *    the virtual ids and then finally the PARTITION_COUNT.
		 * 5) Everyone checks their own node to see the virtual id "suggested"
		 *    by the master.
		 * 6) Try the suggested virtual id, otherwise, scan for a free one.
		 */
		try {
			m_zk.create(BASE_DIR + PROCESS_HEALTH_DIR, 
					  (new JSONObject(isHealthy())).toString().getBytes(),
					  Ids.OPEN_ACL_UNSAFE,
					  CreateMode.EPHEMERAL_SEQUENTIAL);
			
			Stat partitionDone = null;
			while (partitionDone == null) {
				partitionDone = m_zk.exists(BASE_DIR + PARTITION_COUNT, this);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		if (event.getPath().contains(BARRIER_DIR)) {
			synchronized (m_barrierObject) {
				m_barrierObject.notifyAll();				
			}
		}
		else if (event.getPath().contains(PARTITION_COUNT)) {
			m_partitionCountSet.signal();
		}
	}
	
	public int getSuperStep() {
		return m_currentSuperStep;
	}
}
