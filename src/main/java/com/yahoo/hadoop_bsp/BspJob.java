package com.yahoo.hadoop_bsp;

import java.io.IOException;

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
	/**
	 *  Constructor.
	 * @param conf user-defined configuation
	 * @param jobName user-defined job name
	 * @throws IOException
	 */
	public BspJob(
			Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
	}
	
	/**
	 * The mapper that will execute the BSP tasks.  Since this mapper will
	 * not be passing data by key-value pairs through the MR framework, the 
	 * types are irrelevant.
	 * 
	 * @author aching
	 */
	public static class BspMapper 
		extends Mapper<Object, Object, Object, Object> {
		
		@Override
		public void setup(Context context) 
			throws IOException, InterruptedException {
			/*
			 * Do some initial setup, but mainly decide whether to load from a 
			 * checkpoint or from the InputFormat.
			 */
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
