package com.yahoo.hadoop_bsp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.yahoo.hadoop_bsp.examples.TestMsgVertex;
import com.yahoo.hadoop_bsp.examples.TestPageRankVertex;
import com.yahoo.hadoop_bsp.examples.TestSuperstepVertex;
import com.yahoo.hadoop_bsp.examples.TestVertexInputFormat;
import com.yahoo.hadoop_bsp.examples.TestVertexReader;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class HadoopBspTest extends TestCase implements Watcher {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public HadoopBspTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(HadoopBspTest.class);
    }
    
    @Override
    public void setUp() {
    	try {
            String zkList = System.getProperty("prop.zookeeper.list");
            if (zkList == null) {
            	return;
            }
			ZooKeeperExt zooKeeperExt = 
				new ZooKeeperExt(zkList, 30*1000, this);
			List<String> rootChildren = zooKeeperExt.getChildren("/", false);
			for (String rootChild : rootChildren) {
				if (rootChild.startsWith("_hadoopBsp")) {
					List<String> children = 
						zooKeeperExt.getChildren("/" + rootChild, false);
					for (String child: children) {
						if (child.contains("job_local_")) {
							System.out.println("Cleaning up /_hadoopBs/" +
									           child);
							zooKeeperExt.deleteExt(
								"/_hadoopBsp/" + child, -1, true);
						}
					}
				}
			}
			zooKeeperExt.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
    
    /**
     * Just instantiate the vertex (all functions are implemented) and the 
     * VertexInputFormat using reflection.
     * 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws InterruptedException 
     * @throws IOException 
     * @throws InvocationTargetException 
     * @throws IllegalArgumentException 
     * @throws NoSuchMethodException 
     * @throws SecurityException 
     */
    public void testInstantiateVertex() 
    	throws InstantiationException, IllegalAccessException, 
    	IOException, InterruptedException, IllegalArgumentException, 
    	InvocationTargetException, SecurityException, NoSuchMethodException {
    	System.out.println("testInstantiateVertex: java.class.path=" + 
    			           System.getProperty("java.class.path"));
    	java.lang.reflect.Constructor<?> ctor = 
    		TestSuperstepVertex.class.getConstructor();
    	assertNotNull(ctor);
    	TestSuperstepVertex test = 
    		(TestSuperstepVertex) ctor.newInstance();
        System.out.println("testInstantiateVertex: superstep=" + 
        		           test.getSuperstep());
        TestVertexInputFormat inputFormat = 
        	TestVertexInputFormat.class.newInstance();
        List<InputSplit> splitArray = inputFormat.getSplits(1);
        ByteArrayOutputStream byteArrayOutputStream = 
        	new ByteArrayOutputStream();
        DataOutputStream outputStream = 
        	new DataOutputStream(byteArrayOutputStream);
        ((Writable) splitArray.get(0)).write(outputStream);
        System.out.println("testInstantiateVertex: Example output split = " + 
        	byteArrayOutputStream.toString());
    }
    
    /**
     * Run a sample BSP job locally and test supersteps.
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public void testBspSuperStep() 
    	throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        /* Allow this test to be run on a real Hadoop setup */
        String jobTracker = System.getProperty("prop.mapred.job.tracker");
        String jarLocation = System.getProperty("prop.jarLocation");
        if (jobTracker != null) {
        	System.out.println("testBspSuperstep: Sending job to job tracker " +
			           jobTracker + " with jar path " + jarLocation);
            conf.set("mapred.job.tracker", jobTracker);
            conf.set("mapred.jar", jarLocation);
        }
        else {
        	System.out.println("testBspSuperStep: Using local job runner with " + 
        			           "location " + jarLocation + "...");
            conf.set("mapred.jar", jarLocation);
        }
        conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
        conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
        conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        String zkList = System.getProperty("prop.zookeeper.list");
        if (zkList != null) {
        	conf.set(BspJob.BSP_ZOOKEEPER_LIST, zkList);	
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        /* GeneratedInputSplit will generate 5 vertices */
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
    	conf.setClass("bsp.vertexClass", 
    				  TestSuperstepVertex.class, 
    				  HadoopVertex.class);
    	conf.setClass("bsp.inputSplitClass", 
    				  BspInputSplit.class, 
    				  InputSplit.class);
    	conf.setClass("bsp.vertexInputFormatClass", 
    				  TestVertexInputFormat.class,
    				  VertexInputFormat.class);
        conf.setClass("bsp.indexClass",
                      LongWritable.class,
                      WritableComparable.class);
    	BspJob<Integer, String, String> bspJob = 
    		new BspJob<Integer, String, String>(conf, "testBspSuperStep");
       	Path outputPath = new Path("/tmp/testBspSuperStepOutput");    	
    	hdfs.delete(outputPath, true);
    	FileOutputFormat.setOutputPath(bspJob, outputPath);
    	assertTrue(bspJob.run());
    }
    
    /**
     * Run a sample BSP job locally and test messages.
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public void testBspMsg() 
        throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        /* Allow this test to be run on a real Hadoop setup */
        String jobTracker = System.getProperty("prop.mapred.job.tracker");
        String jarLocation = System.getProperty("prop.jarLocation");
        if (jobTracker != null) {
            System.out.println("testBspMsg: Sending job to job tracker " +
                       jobTracker + " with jar path " + jarLocation);
            conf.set("mapred.job.tracker", jobTracker);
            conf.set("mapred.jar", jarLocation);
        }
        else {
            System.out.println("testBspMsg: Using local job runner with " + 
                               "location " + jarLocation + "...");
            conf.set("mapred.jar", jarLocation);
        }
        conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
        conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
        conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        String zkList = System.getProperty("prop.zookeeper.list");
        if (zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, zkList);    
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        /* GeneratedInputSplit will generate 5 vertices */
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass("bsp.vertexClass", 
                      TestMsgVertex.class, 
                      HadoopVertex.class);
        conf.setClass("bsp.inputSplitClass", 
                      BspInputSplit.class, 
                      InputSplit.class);
        conf.setClass("bsp.vertexInputFormatClass", 
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass("bsp.indexClass",
                      LongWritable.class,
                      WritableComparable.class);
        BspJob<Integer, String, String> bspJob = 
            new BspJob<Integer, String, String>(conf, "testBspMsg");
        Path outputPath = new Path("/tmp/testBspMsgOutput");        
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
    }
    
    /**
     * Run a sample BSP job locally and test PageRank.
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public void testBspPageRank() 
        throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        /* Allow this test to be run on a real Hadoop setup */
        String jobTracker = System.getProperty("prop.mapred.job.tracker");
        String jarLocation = System.getProperty("prop.jarLocation");
        if (jobTracker != null) {
            System.out.println("testBspJob: Sending job to job tracker " +
                       jobTracker + " with jar path " + jarLocation);
            conf.set("mapred.job.tracker", jobTracker);
            conf.set("mapred.jar", jarLocation);
        }
        else {
            System.out.println("testBspPageRank: Using local job runner with " + 
                               "location " + jarLocation + "...");
            conf.set("mapred.jar", jarLocation);
        }
        conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
        conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
        conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        String zkList = System.getProperty("prop.zookeeper.list");
        if (zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, zkList);    
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        /* GeneratedInputSplit will generate 5 vertices */
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass("bsp.vertexClass", 
                      TestPageRankVertex.class, 
                      HadoopVertex.class);
        conf.setClass("bsp.inputSplitClass", 
                      BspInputSplit.class, 
                      InputSplit.class);
        conf.setClass("bsp.vertexInputFormatClass", 
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass("bsp.indexClass",
                      LongWritable.class,
                      WritableComparable.class);
        BspJob<Integer, String, String> bspJob = 
            new BspJob<Integer, String, String>(conf, "testBspPageRank");
        Path outputPath = new Path("/tmp/testBspPageRankOutput");        
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
    }
    
    
	public void process(WatchedEvent event) {
		return;
	}
}
