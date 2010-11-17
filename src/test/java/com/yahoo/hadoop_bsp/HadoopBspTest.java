package com.yahoo.hadoop_bsp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

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
	 * Sample BSP application.
	 * 
	 * @author aching
	 *
	 * @param <V>
	 * @param <E>
	 * @param <M>
	 */
	public static final class TestBsp extends 
		HadoopVertex<String, String, Integer, Integer> {
	    public void compute() {
	    	if (getSuperstep() > 3) {
	    		voteToHalt();
	        }
	    }
	}
	
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
			ZooKeeperExt zooKeeperExt = 
				new ZooKeeperExt("localhost:2181", 30*1000, this);
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
    	java.lang.reflect.Constructor<?> ctor = TestBsp.class.getConstructor();
    	assertNotNull(ctor);
    	TestBsp test = (TestBsp) ctor.newInstance();
        System.out.println(test.getSuperstep());
        TestVertexInputFormat inputFormat = 
        	TestVertexInputFormat.class.newInstance();
        List<InputSplit> splitArray = inputFormat.getSplits(1);
        ByteArrayOutputStream byteArrayOutputStream = 
        	new ByteArrayOutputStream();
        DataOutputStream outputStream = 
        	new DataOutputStream(byteArrayOutputStream);
        ((Writable) splitArray.get(0)).write(outputStream);
        System.out.println("Example output split = " + 
        	byteArrayOutputStream.toString());
    }
    
    /**
     * Run a sample BSP job locally.
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public void testBspJob() 
    	throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
        conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
        conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 2);
        conf.setInt(BspJob.BSP_POLL_MSECS, 5*1000);
        conf.set(BspJob.BSP_ZOOKEEPER_LIST, "localhost:2181");
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, 60000);
        /* GeneratedInputSplit will generate 5 vertices */
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
    	conf.setClass("bsp.vertexClass", TestBsp.class, HadoopVertex.class);
    	conf.setClass("bsp.inputSplitClass", 
    				  BspInputSplit.class, 
    				  InputSplit.class);
    	conf.setClass("bsp.vertexInputFormatClass", 
    				  TestVertexInputFormat.class,
    				  VertexInputFormat.class);
    	BspJob<Integer, String, String> bspJob = 
    		new BspJob<Integer, String, String>(conf, "testBspJob");
       	Path outputPath = new Path("/tmp/testBspJobOutput");    	
    	hdfs.delete(outputPath, true);
    	FileOutputFormat.setOutputPath(bspJob, outputPath);
    	bspJob.run();
    }
    
	public void process(WatchedEvent event) {
		return;
	}
}
