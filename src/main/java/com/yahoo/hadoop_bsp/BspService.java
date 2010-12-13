package com.yahoo.hadoop_bsp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.net.InetAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Zookeeper-based implementation of {@link CentralizedService}.
 * @author aching
 *
 */
public class BspService<I> implements 
	CentralizedService<I>, CentralizedServiceMaster<I>, Watcher {
	/** Private ZooKeeper instance that implements the service */
	private ZooKeeperExt m_zk = null;
	/** My virtual identity in the group */
	private String m_myVirtualId = null;
	/** My input split */
	private InputSplit m_myInputSplit = null;
	/** Registration synchronization */
	private BspEvent m_partitionCountSet = new PredicateLock();
	/** Barrier synchronization */
	private BspEvent m_barrierDone = new PredicateLock();
	/** Barrier children synchronization */
	private BspEvent m_barrierChildrenChanged = new PredicateLock();
	/** Finished children synchronization */
	private BspEvent m_finishedChildrenChanged= new PredicateLock();
    /** Master children synchronization */
    private BspEvent m_masterChildrenChanged= new PredicateLock();
	/** Partition count */
	private Integer m_partitionCount;
	/** Configuration of the job*/
	private Configuration m_conf;
	/** Job context (mainly for progress) */
    private Context m_context;
	/** The partition map */
	private NavigableSet<Partition<I>> m_partitionSet = null;
	/** Cached superstep */
	long m_cachedSuperstep = -1;
	/** Cached aggregate number of vertices in the entire application */
	long m_totalVertices = -1;
	/** Job id, to ensure uniqueness */
	String m_jobId;
	/** Task partition, to ensure uniqueness */
	int m_taskPartition;
	/** My process health znode */
	String m_myHealthZnode;
	/** Master thread */
	Thread m_masterThread;
    /** Partition to compare with */
    Partition<I> comparePartition = new Partition<I>("", -1, null);
	/** Master should stop trying to become the leader? */
	boolean m_masterThreadGiveUpLeader = false;
	/** Lock to protect m_masterThreadGiveUpLeader */
    Lock m_masterThreadGiveUpLeaderLock = new ReentrantLock();
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
	public static final String BARRIER_NODE = "_barrierDone";
	public static final String SUPERSTEP_NODE = "/_superstep";
	public static final String PROCESS_HEALTH_DIR = "/_processHealth";
	public static final String MASTER_DIR = "/_master";
	public static final String PARTITION_COUNT_NODE = "/_partitionCount";
	public static final String VIRTUAL_ID_DIR = "/_virtualIdDir";
	public static final String JOB_STATE_NODE = "/_jobState";
	public static final String FINISHED_NODE = "/_finished";

	public static final String INFO_DONE_KEY = "done_key";
	public static final String INFO_NUM_VERTICES = "numVertices_key";
	
	private final String BASE_PATH;
	private final String BARRIER_PATH;
	private final String SUPERSTEP_PATH;
	private final String PROCESS_HEALTH_PATH;
	private final String MASTER_PATH;
	private final String PARTITION_COUNT_PATH;
	private final String VIRTUAL_ID_PATH;
	private final String JOB_STATE_PATH;
	private final String FINISHED_PATH;
	
	public BspService(String serverPortList, int sessionMsecTimeout, 
		Context context) throws IOException, KeeperException, InterruptedException, 
		JSONException {
	    m_context = context;
		m_conf = context.getConfiguration();
		m_jobId = m_conf.get("mapred.job.id", "Unknown Job");
		m_taskPartition = m_conf.getInt("mapred.task.partition", -1);

		BASE_PATH = BASE_DIR + "/" + m_jobId;
		BARRIER_PATH = BASE_PATH + BARRIER_DIR;
		SUPERSTEP_PATH = BASE_PATH + SUPERSTEP_NODE;
		PROCESS_HEALTH_PATH = BASE_PATH + PROCESS_HEALTH_DIR;
		MASTER_PATH = BARRIER_PATH + MASTER_DIR;
		PARTITION_COUNT_PATH = BASE_PATH + PARTITION_COUNT_NODE;
		VIRTUAL_ID_PATH = BASE_PATH + VIRTUAL_ID_DIR;
		JOB_STATE_PATH = BASE_PATH + JOB_STATE_NODE;
		FINISHED_PATH = BASE_PATH + FINISHED_NODE;	
		
		LOG.info("BspService: Connecting to ZooKeeper with " + m_jobId + ", " +
		         m_taskPartition + " on " + serverPortList);
	    m_zk = new ZooKeeperExt(serverPortList, sessionMsecTimeout, this);
	    m_masterThread = new MasterThread<I>(this);
	    m_masterThread.start();
	}

	/** 
	 * Intended to check the health of the node.  For instance, can it ssh, 
	 * dmesg, etc. For now, does nothing.
	 */
	public boolean isHealthy() {
		return true;
	}
	
	/**
	 * Calculate the input split and write it to zookeeper.
	 * @param numSplits
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public List<InputSplit> generateInputSplits(int numSplits) { 
		try {
			@SuppressWarnings({
					"rawtypes", "unchecked" })
			Class<VertexInputFormat> vertexInputFormatClass = 
				(Class<VertexInputFormat>) 
				 m_conf.getClass("bsp.vertexInputFormatClass", 
						  	 	 VertexInputFormat.class);
			@SuppressWarnings("unchecked")
			List<InputSplit> splits =
					vertexInputFormatClass.newInstance().getSplits(m_conf, numSplits);
			return splits;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public InputSplit getInputSplit() {
		return m_myInputSplit;
	}
		
	/**
	 * If the master decides that this job doesn't have the resources to 
	 * continue, it can fail the job.
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	public synchronized void masterSetJobState(State state) { 
		m_currentState = state;
		try {
			m_zk.createExt(JOB_STATE_PATH, 
						   state.toString().getBytes(),
						   Ids.OPEN_ACL_UNSAFE, 
						   CreateMode.PERSISTENT,
						   true);
		} catch (KeeperException.NodeExistsException e) {
			try {
				m_zk.setData(JOB_STATE_PATH, state.toString().getBytes(), -1);
			} catch (Exception e1) {
				throw new RuntimeException(e1);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
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
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public int masterCreatePartitions() {
		try {
			if (m_zk.exists(PARTITION_COUNT_PATH, false) != null) {
				LOG.info(PARTITION_COUNT_PATH + 
						 " already exists, no need to create");
				return Integer.parseInt(
					new String(m_zk.getData(PARTITION_COUNT_PATH, false, null)));
			}
		} catch (KeeperException.NoNodeException e) {
			LOG.info("masterCreatePartitions: Need to create the " + 
					 "partitions at " + PARTITION_COUNT_PATH);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		int maxPollAttempts = m_conf.getInt(BspJob.BSP_POLL_ATTEMPTS, 
											BspJob.DEFAULT_BSP_POLL_ATTEMPTS);
		int initialProcs = m_conf.getInt(BspJob.BSP_INITIAL_PROCESSES, -1);
		int minProcs = m_conf.getInt(BspJob.BSP_MIN_PROCESSES, -1);
		float minPercentResponded = 
			m_conf.getFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 0.0f);
		List<String> procsReported = new ArrayList<String>();
		int msecsPollPeriod = m_conf.getInt(BspJob.BSP_POLL_MSECS, 
											BspJob.DEFAULT_BSP_POLL_MSECS);
		boolean failJob = true;
		int pollAttempt = 0;
		while (pollAttempt < maxPollAttempts) {
			try {
				procsReported = m_zk.getChildren(PROCESS_HEALTH_PATH, false);
				if ((procsReported.size() * 100.0f / initialProcs) >= 
					minPercentResponded) {
					failJob = false;
					break;
				}
			} catch (KeeperException.NoNodeException e) {
				LOG.info("masterCreatePartitions: No node " + 
						 PROCESS_HEALTH_PATH + " exists: " + 
						 e.getMessage());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			LOG.info("masterCreatePartitions: Sleeping for " + 
					 msecsPollPeriod + " msecs and used " + pollAttempt + 
					 " of " + maxPollAttempts + " attempts.");
			try {
				Thread.sleep(msecsPollPeriod);
			} catch (InterruptedException e) {
				LOG.error("masterCreatePartitions: Sleep interrupted!");
			}
			++pollAttempt;
		}
		if (failJob) {
			masterSetJobState(State.FAILED);
			throw new RuntimeException(
			    "Did not receive enough processes in time (only " + 
			    procsReported.size() + " of " + minProcs + " required)");
		}
		
		int healthyProcs = 0;
		for (String proc : procsReported) {
			try {
				String jsonObject = 
					new String(m_zk.getData(PROCESS_HEALTH_PATH + "/" + proc, 
							   false, 
							   null));
				LOG.info("masterCreatePartitions: Health of " + proc + " = "
						 + jsonObject);
				Boolean processHealth = 
					(Boolean) JSONObject.stringToValue(jsonObject);
				if (processHealth.booleanValue()) {
					++healthyProcs;
				}
			} catch (KeeperException.NoNodeException e) {
				LOG.error("masterCreatePartitions: Process at " + proc + 
						  " between retrieving children and getting znode");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (healthyProcs < minProcs) {
			masterSetJobState(State.FAILED);
			throw new RuntimeException(
				"Only " + Integer.toString(healthyProcs) + " available when " + 
				Integer.toString(minProcs) + " are required.");
		}

		/*
		 *  When creating znodes, in case the master has already run, resume 
		 *  where it left off.
		 */
		try {
		    m_zk.create(FINISHED_PATH, 
		                null, 
		                Ids.OPEN_ACL_UNSAFE, 
		                CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
		    LOG.info("masterCreatePartitions: Finished path " + FINISHED_PATH + 
		             " already exists.");
		} catch (Exception e) {
		    throw new RuntimeException(e);
		}
		
		try {
			m_zk.create(VIRTUAL_ID_PATH, 
						null,
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("masterCreatePartitions: Node " + VIRTUAL_ID_PATH + 
					 " already exists.");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		List<InputSplit> splitArray = generateInputSplits(healthyProcs);
		if (healthyProcs > splitArray.size()) {
		    LOG.warn("Number of inputSplits=" + splitArray.size() +
		             " < " + healthyProcs + "=number of healthy processes");
		    healthyProcs = splitArray.size();
		}
		for (int i = 0; i < healthyProcs; ++i) {
			try {
				ByteArrayOutputStream byteArrayOutputStream = 
					new ByteArrayOutputStream();
				DataOutput outputStream = 
					new DataOutputStream(byteArrayOutputStream);
				((Writable) splitArray.get(i)).write(outputStream);
				m_zk.create(VIRTUAL_ID_PATH + "/" + Integer.toString(i), 
							byteArrayOutputStream.toByteArray(),
					  		Ids.OPEN_ACL_UNSAFE, 
					  		CreateMode.PERSISTENT);
				LOG.info("masterCreatePartitions: Created virtual id " + 
						 VIRTUAL_ID_PATH + "/" + 
						 Integer.toString(i) + " with split " + 
						 byteArrayOutputStream.toString());
			} catch (KeeperException.NodeExistsException e) {
				LOG.info("masterCreatePartitions: Node " + 
						 VIRTUAL_ID_PATH + "/" + 
						 Integer.toString(i) +" already exists.");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		try {
			/* 
			 * The first superstep is -1, this is so that it can be used 
			 * as a barrier so that the communications service can be started. 
			 */
			m_zk.create(SUPERSTEP_PATH,
						Integer.toString(-1).getBytes(),
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("masterCreatePartitions: Node " + SUPERSTEP_PATH + 
					 " already exists.");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		try {
			m_zk.create(PARTITION_COUNT_PATH, 
						Integer.toString(healthyProcs).getBytes(),
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
			LOG.info("masterCreatePartitions: Created partition count path " + 
					 PARTITION_COUNT_PATH + " with count = " + healthyProcs);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("masterCreatePartitions: Node " + PARTITION_COUNT_PATH + " already exists.");	
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		return healthyProcs;
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
			m_myHealthZnode =
				m_zk.createExt(
					PROCESS_HEALTH_PATH + "/" 
					+ InetAddress.getLocalHost().getHostName() + "-" + 
					m_taskPartition, 
					Boolean.toString(isHealthy()).getBytes(),
					Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL,
					true);
			LOG.info("setup: Created my health node: " + m_myHealthZnode);
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
			
		    m_cachedSuperstep = getSuperStep();
		    LOG.info("setup: Using super step " + m_cachedSuperstep + 
		    		 " with partition count "+ m_partitionCount);
		    
		    /*
		     *  Try to claim a virtual id in a polling period (use it to store
		     *  your hostname and port number for the messaging service.
		     */
		    List<String> virtualIdList = null;
		    boolean reservedNode = false;
		    JSONArray hostnamePort = new JSONArray();
		    hostnamePort.put(InetAddress.getLocalHost().getHostName());
		    int randomRpcPort = (int) (30000 * Math.random()) + 30000;
		    int finalRpcPort = m_conf.getInt(BspJob.BSP_RPC_INITIAL_PORT, 
		    								 randomRpcPort);
		    finalRpcPort += m_taskPartition;
		    LOG.info("setup: Using port " + finalRpcPort);
		    hostnamePort.put(finalRpcPort);
		    while ((getJobState() != State.FAILED) &&
		    	   (getJobState() != State.FINISHED)) {
		    	virtualIdList = m_zk.getChildren(VIRTUAL_ID_PATH, false);
		    	for (String virtualId : virtualIdList) {
		    		try {
		    			m_zk.create(VIRTUAL_ID_PATH + "/" + virtualId + 
		    					    "/reserved", 
		    					    hostnamePort.toString().getBytes(),
		    					    Ids.OPEN_ACL_UNSAFE, 
		    					    CreateMode.EPHEMERAL);
			    		LOG.info("setup: Reserved virtual id " + virtualId +
			    				 " with hostname port = " + 
			    				 hostnamePort.toString());
		    			reservedNode = true;
		    			m_myVirtualId = virtualId;
		    			break;
		    		} catch (KeeperException.NodeExistsException e) {
		    			LOG.info("setup: Failed to reserve " + virtualId);
		    		}
		    	}
		    	if (reservedNode) {
					@SuppressWarnings("unchecked")
					Class<? extends Writable> inputSplitClass = 
						(Class<Writable>) m_conf.getClass("bsp.inputSplitClass", 
									    				  InputSplit.class);
					m_myInputSplit = (InputSplit) ReflectionUtils.newInstance(inputSplitClass, m_conf);
					byte [] splitArray = m_zk.getData(
				    		VIRTUAL_ID_PATH + "/" + m_myVirtualId, false, null);
					LOG.info("setup: For " + VIRTUAL_ID_PATH + "/" + 
							 m_myVirtualId + ", got '" + splitArray + "'");
				  InputStream input = 
				    	new ByteArrayInputStream(splitArray);
					((Writable) m_myInputSplit).readFields(
						new DataInputStream(input));
		    		return;
		    	}
		    	Thread.sleep(
		    		m_conf.getInt(BspJob.BSP_POLL_MSECS, 
		    				      BspJob.DEFAULT_BSP_POLL_MSECS));
		    	m_context.progress();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}
	public boolean barrier(long verticesDone, long verticesTotal) {
		/* Note that this barrier blocks until success.  It would be best if 
		 * it were interruptible if for instance there was a failure. */
		
		/*
		 * Master will coordinate the barriers and aggregate "doneness".
		 * Each process writes its virtual id to the barrier superstep and 
		 * encodes the number of done vertices and total vertices.  
		 * Then it waits for the master to say whether to stop or not.
		 */
		try {
			JSONArray doneTotalArray = new JSONArray();
			doneTotalArray.put(verticesDone);
			doneTotalArray.put(verticesTotal);
			m_zk.createExt(BARRIER_PATH + "/" + 
					       Long.toString(m_cachedSuperstep) + "/" +
					       m_myVirtualId, 
					       doneTotalArray.toString().getBytes(),
						   Ids.OPEN_ACL_UNSAFE, 
						   CreateMode.EPHEMERAL,
						   true);
			String barrierNode = BARRIER_PATH + "/" + 
								 Long.toString(m_cachedSuperstep) + 
								 "/" + BARRIER_NODE; 
			if (m_zk.exists(barrierNode, true) == null) {
				m_barrierDone.waitForever();
				m_barrierDone.reset();
			}
			JSONObject infoObject = new JSONObject(
			    new String(m_zk.getData(barrierNode, false, null)));
			boolean done = infoObject.optBoolean(INFO_DONE_KEY);
			m_totalVertices = infoObject.optLong(INFO_NUM_VERTICES);
			LOG.info("barrier: Completed superstep " + 
					 m_cachedSuperstep + " with done=" + done + 
					 ", numVertices=" + m_totalVertices);
			++m_cachedSuperstep;
			return done;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean becomeMaster() {
	    /*
	     * Create my bid to become the master, then try to become the worker 
	     * or return false.
	     */
	    String myBid = null;
	    try {
	        myBid = 
	            m_zk.createExt(
	                MASTER_PATH + "/" + Integer.toString(m_taskPartition), 
	                null,
	                Ids.OPEN_ACL_UNSAFE, 
	                CreateMode.EPHEMERAL_SEQUENTIAL, 
	                true);
	    } catch (Exception e) {
	        throw new RuntimeException(e);
	    }
	    while (true) {
	        try {
	            m_masterThreadGiveUpLeaderLock.lock();
	            if (m_masterThreadGiveUpLeader == true) {
	                m_masterThreadGiveUpLeaderLock.unlock();
                    LOG.info("becomeMaster: Give up trying to be the master!");
	                return false;
	            }
                m_masterThreadGiveUpLeaderLock.unlock();
	            List<String> masterChildArr = 
	                m_zk.getChildrenExt(MASTER_PATH, true, true, true);
	            LOG.info("becomeMaster: First child is '" + 
	                     masterChildArr.get(0) + "' and my bid is '" +
	                     myBid + "'");
	            if (masterChildArr.get(0).equals(myBid)) {
	                LOG.info("becomeMaster: Became the master!");
	                return true;
	            }
	            LOG.info("becomeMaster: Waiting to become the master...");
	            m_masterChildrenChanged.waitForever();
	            m_masterChildrenChanged.reset();
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
	}
	
	/**
	 * Master will watch the children until the number of children is the same
	 * as the number of partitions.  Then it will determine whether to finish 
	 * the application or not.
	 * @return true if done with application
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws JSONException 
	 */
	public boolean masterBarrier(long superstep, int partitions) {
		String barrierChildrenNode = 
			BARRIER_PATH + "/" + Long.toString(superstep); 

		try {
			m_zk.createExt(barrierChildrenNode, 
						   null, 
					       Ids.OPEN_ACL_UNSAFE, 
					       CreateMode.PERSISTENT,
					       true);
		} catch (KeeperException.NodeExistsException e) {
			LOG.info("masterBarrier: Node " + barrierChildrenNode + 
					 " already exists, no need to create");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		List<String> childrenList = null;
		long verticesDone = -1;
		long verticesTotal = -1;
		while (true) {
			try {
				childrenList = m_zk.getChildren(barrierChildrenNode, true);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			LOG.info("masterBarrier: Got " + childrenList.size() + " of " +
					 partitions + " children from " + barrierChildrenNode);
			if (childrenList.size() == partitions) {
				boolean allReachedBarrier = true;
				verticesDone = 0;
				verticesTotal = 0;
				for (String child : childrenList) {
					try {
						JSONArray jsonArray = new JSONArray(
							new String(m_zk.getData(barrierChildrenNode + "/" + child, false, null)));
						verticesDone += jsonArray.getLong(0);
						verticesTotal += jsonArray.getLong(1);
						LOG.info("masterBarrier: Got " + jsonArray.getLong(0) + 
								 " of " + jsonArray.getLong(1) + 
								 " vertices done for " + barrierChildrenNode + "/" 
								 + child);
					} catch (KeeperException.NoNodeException e) {
						allReachedBarrier = false;
						LOG.info("masterBarrier: Node " + barrierChildrenNode + 
								 "/" + child + " was good, but died.");
						break;
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					continue;
				}
				if (allReachedBarrier) {
					break;	
				}
			}
			LOG.info("masterBarrier: Waiting for the children of " + 
					 barrierChildrenNode + " to change since only got " +
					 childrenList.size() + " nodes.");
			m_barrierChildrenChanged.waitForever();
			m_barrierChildrenChanged.reset();
		}

		boolean applicationDone = false;
		if (verticesDone == verticesTotal) {
			applicationDone = true;
		}
		LOG.info("masterBarrier: Aggregate got " + verticesDone + " of " + 
				 verticesTotal + " halted on superstep = " + superstep + 
				 " (application done = " + applicationDone + ")");
		setSuperStep(superstep + 1);
		String barrierNode = BARRIER_PATH + "/" + Long.toString(superstep) + 
		 					 "/" + BARRIER_NODE;
		/* Let everyone know the overall application state through the barrier */
		try {
		    JSONObject globalInfoObject = new JSONObject();
		    globalInfoObject.put(INFO_DONE_KEY, applicationDone);
		    globalInfoObject.put(INFO_NUM_VERTICES, verticesTotal);
			m_zk.create(barrierNode,
			            globalInfoObject.toString().getBytes(),
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		/* Clean up the old barriers */
		if ((superstep - 1) >= 0) {
			try {
				LOG.info("masterBarrier: Cleaning up old barrier " + 
						BARRIER_PATH + "/" + Long.toString(superstep - 1));
				m_zk.deleteExt(BARRIER_PATH + "/" + 
						       Long.toString(superstep - 1),
						       -1, 
						       true);
			} catch (KeeperException.NoNodeException e) {
				LOG.warn("masterBarrier: Already cleaned up " + 
						 BARRIER_PATH + "/" + Long.toString(superstep - 1));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		return applicationDone;
	}
	
	public long getSuperStep() {
	    try {
			return (Integer) JSONObject.stringToValue(
			    	new String(m_zk.getData(SUPERSTEP_PATH, false, null)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public long getTotalVertices() {
	    return m_totalVertices;
	}
	
	public void setSuperStep(long superStep) {
	    try {
	    	m_zk.setData(SUPERSTEP_PATH, 
	    				 Long.toString(superStep).getBytes(),
	    			     -1);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
	
	public void cleanup() {
		/*
		 * All processes should denote they are done by adding special
		 * znode.  Once the number of znodes equals the number of partitions,
		 * the master will clean up the ZooKeeper znodes associated with this
		 * job.
		 */
		try {
			String finalFinishedPath = 
				m_zk.createExt(
					FINISHED_PATH + "/" + m_taskPartition, 
					null, 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT, 
					true);
	         LOG.info("cleanup: Notifying master its okay to cleanup with " +
                     finalFinishedPath);
		} catch (KeeperException.NodeExistsException e) { 
			LOG.info("cleanup: Couldn't create finished node '" +
					FINISHED_PATH + "/" + m_taskPartition + "'");
		} catch (Exception e) {
		    // cleanup phase -- just log the error
		    LOG.error(e.getMessage());
		}
		m_masterThreadGiveUpLeaderLock.lock();
		m_masterThreadGiveUpLeader = true;
		m_masterThreadGiveUpLeaderLock.unlock();
		m_masterChildrenChanged.signal();
		try {
		    m_masterThread.join();
		} catch (InterruptedException e) {
		    // cleanup phase -- just log the error
		    LOG.error("cleanup: Master thread couldn't join");
		}
		try {
		    m_zk.close();
		} catch (InterruptedException e) {
		    // cleanup phase -- just log the error
		    LOG.error("cleanup: Zookeeper failed to close");
		}
	}
	
	public void masterCleanup(int partitions) {
		List<String> childrenList = null;
		while (true) {
			try {
			    /*
			     * FINISHED_PATH already exists from masterCreatePartitions()
			     * so there should be no Exceptions.
			     */
				childrenList = m_zk.getChildren(FINISHED_PATH, true);
				LOG.info("masterCleanup: Got " + childrenList.size() + " of " +
						 partitions + " children from " + FINISHED_PATH);
				if (childrenList.size() == partitions) {
					break;	
				}
				LOG.info("masterCleanup: Waiting for the children of " + 
						 FINISHED_PATH + " to change since only got " +
						 childrenList.size() + " nodes.");
			} 
			catch (Exception e) {
			    // we are in the cleanup phase -- just log the error
			    LOG.error(e.getMessage());
			    return;
			}

			m_finishedChildrenChanged.waitForever();
			m_finishedChildrenChanged.reset();
		}
		
		/* 
		 * At this point, all processes have acknowledged the cleanup, 
		 * and the master can do any final cleanup 
		 */
		try {
			LOG.info("masterCleanup: Removing the following path and all " + 
					 "children - " + BASE_PATH);
			m_zk.deleteExt(BASE_PATH, -1, true);
		} catch (Exception e) {
			LOG.error("masterCleanup: Failed to do cleanup of " + BASE_PATH);
		}
	}
	
	public void process(WatchedEvent event) {
		LOG.info("process: Got a new event, path = " + event.getPath() + 
				 ", type = " + event.getType() + ", state = " + 
				 event.getState());
		/* Nothing to do unless it is a disconnet */
		if (event.getPath() == null) {
		    if (event.getType() == EventType.None &&
		            event.getState() == KeeperState.Disconnected) {
		        m_partitionCountSet.signal();
			    m_barrierDone.signal();
		        m_barrierChildrenChanged.signal();
		        m_finishedChildrenChanged.signal();
		        m_masterChildrenChanged.signal();
		    }
			return;
		}
		
		if (event.getPath().equals(PARTITION_COUNT_PATH)) {
			m_partitionCountSet.signal();
		}
		else if (event.getPath().equals(JOB_STATE_PATH)) {
			try {
				synchronized (this) {
					m_currentState = State.valueOf(
					    new String(m_zk.getData(event.getPath(), true, null)));					
				}
			} catch (KeeperException.NoNodeException e) {
				LOG.error("process: " + JOB_STATE_PATH + " was unusually removed.");
			} catch (Exception e) {
				// Shouldn't ever happen
				m_currentState = State.FAILED;
			}
		}
		else if (event.getPath().contains(BARRIER_PATH) &&
				 event.getType() == EventType.NodeCreated) {
			LOG.info("process: m_barrierDone signaled");
			m_barrierDone.signal();
		}
		else if (event.getPath().contains(BARRIER_PATH) &&
				 event.getType() == EventType.NodeChildrenChanged) {
			LOG.info("process: m_barrierChildrenChanged signaled");
			m_barrierChildrenChanged.signal();
		}
        else if (event.getPath().contains(FINISHED_PATH) &&
                event.getType() == EventType.NodeChildrenChanged) {
           LOG.info("process: m_finishedChildrenChanged signaled");
           m_finishedChildrenChanged.signal();
       }
        else if (event.getPath().contains(MASTER_PATH) &&
                event.getType() == EventType.NodeChildrenChanged) {
           LOG.info("process: m_masterChildrenChanged signaled");
           m_masterChildrenChanged.signal();
       }
		else {
			LOG.error("process: Unknown event");
		}
	}

	public void setPartitionMax(I max) {
		String reservedPath = VIRTUAL_ID_PATH + "/" + m_myVirtualId + 
			      "/reserved";
		JSONArray hostnamePort = null;
		try {
			hostnamePort = new JSONArray(new String
				(m_zk.getData(reservedPath, false, null)));
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			DataOutput output = new DataOutputStream(outputStream);
			((Writable) max).write(output);
			hostnamePort.put(outputStream.toString("UTF-8"));
			LOG.info("setPartitionMax: Wrote JSON Array " + hostnamePort + 
			         " to " + reservedPath + ", maxIndex=" + max);
			m_zk.setData(reservedPath, hostnamePort.toString().getBytes("UTF-8"), -1);
		} catch (Exception e) {
			LOG.error("setPartitionMax: Failed to set the partition of node " + 
					  reservedPath + " to " + hostnamePort.toString());
		}
	}

	public NavigableSet<Partition<I>> getPartitionSet() {
		if (m_partitionSet == null) {
			m_partitionSet = new TreeSet<Partition<I>>();
			try {
				for (int i = 0; i < m_partitionCount; ++i) {
					String tmpPath = VIRTUAL_ID_PATH + "/" + i + "/reserved";
					JSONArray jsonArray = new JSONArray(new String(
						m_zk.getData(tmpPath, false, null)));
					LOG.info("getPartitionSet: Got partition " + 
							 jsonArray.toString() + " from " + tmpPath);
					if (jsonArray.length() != 3) {
						throw new RuntimeException(
							"getPartitionSet: Impossible that znode " + 
							tmpPath + " has jsonArray " + jsonArray);
					}
					@SuppressWarnings({"rawtypes", "unchecked" })
			    Class<? extends WritableComparable> indexClass =
			            (Class<? extends WritableComparable>) 
			            m_conf.getClass("bsp.indexClass", 
			                                WritableComparable.class);
          @SuppressWarnings("unchecked")
          I index = (I) indexClass.newInstance();
		      InputStream input = 
		                new ByteArrayInputStream(jsonArray.get(2).toString().getBytes("UTF-8"));
	                 ((Writable) index).readFields(
	                         new DataInputStream(input));
					m_partitionSet.add(
						new Partition<I>(jsonArray.getString(0), 
										 jsonArray.getInt(1), 
										 index));
          LOG.info("getPartitionSet: Partition split point: " + index);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}		
		return m_partitionSet;
	}
	
	public Partition<I> getPartition(I index) {
		if (m_partitionSet == null) {
			getPartitionSet();
		}
		comparePartition.setMaxIndex(index);
		Partition<I> result = m_partitionSet.ceiling(comparePartition);
		if (result == null) {
		    LOG.warn("getPartition: no partition for destination vertex " + index +
		             " -- returning last partition");
		    result = m_partitionSet.last();
		}
		return result;
	}
}
