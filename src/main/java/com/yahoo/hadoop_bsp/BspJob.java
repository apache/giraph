package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Limits the functions that can be called by the user.  Job is too flexible
 * for our needs.  For instance, there are no reduce tasks.
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
	/** Default poll msecs (30 seconds) */
	public static int DEFAULT_BSP_POLL_MSECS = 30000;
	/** Number of poll attempts prior to failing the job (int) */
	public static final String BSP_POLL_ATTEMPTS = "bsp.pollAttempts";
	/** Default poll attempts */
	public static int DEFAULT_BSP_POLL_ATTEMPTS = 3;
	
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
		ArrayList<VertexData<V, E>> m_vertexArray;
		
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
		}
		
		@Override
		public void cleanup(Context context) 
			throws IOException, InterruptedException {
			/*
			 * 1) For every vertex on this mapper, run the compute() function
			 * 2) Wait until all messaging is done.
			 * 3) Check if all vertices are done.  If not goto 2).
			 * 4) Dump output.
			 */
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
