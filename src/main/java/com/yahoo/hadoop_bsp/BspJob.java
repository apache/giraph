package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.json.JSONException;
import org.mortbay.log.Log;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.  For instance, our job should not have any reduce tasks.
 * 
 * @author aching
 */
public class BspJob extends Job {
	/** Minimum number of simultaneous processes before this job can run (int) */
	public static final String BSP_MIN_PROCESSES = "bsp.minProcs";
	/** Initial number of simultaneous tasks started by this job (int) */
	public static final String BSP_INITIAL_PROCESSES = "bsp.maxProcs";
	/** Minimum percent of initial processes that have responded (float) */
	public static final String BSP_MIN_PERCENT_RESPONDED = 
		"bsp.minPercentResponded";
	/** Polling timeout to check on the number of responded tasks (int) */
	public static final String BSP_POLL_MSECS = "bsp.pollMsecs";
	/** Zookeeper list (empty for start up Zookeeper locally) */
	public static final String BSP_ZOOKEEPER_LIST = "bsp.zkList";
	/** Zookeeper session millisecond timeout */
	public static final String BSP_ZOOKEEPER_SESSION_TIMEOUT = 
		"bsp.zkSessionMsecTimeout";
	/** Default poll msecs (30 seconds) */
	public static int DEFAULT_BSP_POLL_MSECS = 30*1000;
	/** Number of poll attempts prior to failing the job (int) */
	public static final String BSP_POLL_ATTEMPTS = "bsp.pollAttempts";
	/** Default poll attempts */
	public static int DEFAULT_BSP_POLL_ATTEMPTS = 3;
	/** Default Zookeeper session millisecond timeout */
	public static int DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT = 30*1000;
	
	/**
	 *  Constructor.
	 * @param conf user-defined configuration
	 * @param jobName user-defined job name
	 * @throws IOException
	 */
	public BspJob(
			Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
		if (conf.getInt(BSP_INITIAL_PROCESSES, -1) < 0) {
			throw new IOException("No valid " + BSP_INITIAL_PROCESSES);
		}
		if (conf.getFloat(BSP_MIN_PERCENT_RESPONDED, 0.0f) <= 0) {
			throw new IOException("No valid " + BSP_MIN_PERCENT_RESPONDED);
		}
		if (conf.getInt(BSP_MIN_PROCESSES, -1) < 0) {
			throw new IOException("No valid " + BSP_MIN_PROCESSES);
		}
		if (conf.get(BSP_ZOOKEEPER_LIST, "").isEmpty()) {
			throw new IOException(
				"Empty zk list not yet supported (future work");
		}
	}
	
	/**
	 * The mapper that will execute the BSP tasks.  Since this mapper will
	 * not be passing data by key-value pairs through the MR framework, the 
	 * types are irrelevant.
	 * 
	 * @author aching
	 * @param <V>
	 * @param <V>
	 */
	public static class BspMapper<V, E>
		extends Mapper<Object, Object, Object, Object> {
		/** Logger */
	    private static final Logger LOG = Logger.getLogger(BspMapper.class);
		/** Data structure for managing vertices */
		ArrayList<VertexData<V, E>> m_vertexArray;
		/** */
		CentralizedService m_service;
		
		/**
		 * Load the vertices from the user-defined RecordReader into our 
		 * vertexArray.
		 */
		public void loadVertices() {
			
		}
		
		@Override
		public void setup(Context context) 
			throws IOException, InterruptedException {
			/*
			 * Do some initial setup, but mainly decide whether to load from a 
			 * checkpoint or from the InputFormat.
			 */
			m_vertexArray = new ArrayList<VertexData<V,E>>();
			Configuration configuration = context.getConfiguration();
			String serverPortList = 
				configuration.get(BspJob.BSP_ZOOKEEPER_LIST, "");
			int sessionMsecTimeout = 
				configuration.getInt(
					BspJob.BSP_POLL_MSECS,
					BspJob.DEFAULT_BSP_ZOOKEEPER_SESSION_TIMEOUT);
				try {
					LOG.info("Starting up BspService...");
					m_service = new BspService(
						serverPortList, sessionMsecTimeout, configuration);
					LOG.info("Registering health of this process...");
					m_service.setup();
				} catch (Exception e) {
					LOG.error(e.getMessage());
					throw new RuntimeException(e);
				}
		}
		
		@Override
		public void map(Object key, Object value, Context context)
			throws IOException, InterruptedException {
			/**
			 * map() simply loads the data from the InputFormat to this mapper.
			 * If a checkpoint exists, this function should not have been
			 * called. 
			 */
			System.out.printf("Key: '%s'\n", key);
			System.out.printf("Value: '%s'\n", value);
			/*
			 * 1) For every vertex on this mapper, run the compute() function
			 * 2) Wait until all messaging is done.
			 * 3) Check if all vertices are done.  If not goto 2).
			 * 4) Dump output.
			 */
		}
		
		@Override
		public void cleanup(Context context) 
			throws IOException, InterruptedException {
			Log.info("Client done.");
			m_service.cleanup();
		}
	}
	
	/**
	 * Runs the actual BSPJob through Hadoop.
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public boolean run() throws IOException, InterruptedException, 
		ClassNotFoundException {
		setNumReduceTasks(0);
		setJarByClass(BspJob.class);
	    setMapperClass(BspMapper.class);
        setInputFormatClass(BspInputFormat.class);
	    return waitForCompletion(true);
	}
	
	/**
	 * Users should set the Vertex implementation they have chosen.
	 */
	public void setVertexClass() {
		
	}
}
