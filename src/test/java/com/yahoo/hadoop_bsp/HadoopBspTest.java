package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class HadoopBspTest extends TestCase {
	
	/**
	 * Sample BSP application.
	 * 
	 * @author aching
	 *
	 * @param <V>
	 * @param <E>
	 * @param <M>
	 */
	public final class TestBsp<V, E, M> extends HadoopVertex<V, E, M> {
	    public void compute() {
	    	if (getSuperstep() > 30) {
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

    /**
     * Just instantiate the vertex (all functions are implemented)
     */
    public void testInstantiateVertex() {
        TestBsp<Integer, String, String> test = 
        	new TestBsp<Integer, String, String>();
        System.out.println(test.getSuperstep());
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
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 1);
        conf.setInt(BspJob.BSP_POLL_MSECS, 1000);
        conf.set(BspJob.BSP_ZOOKEEPER_LIST, "localhost:2221");
        FileSystem hdfs = FileSystem.get(conf);
    	BspJob bspJob = new BspJob(conf, "testBspJob");
       	Path inputPath = new Path("/tmp/testBspJobInput");
       	Path outputPath = new Path("/tmp/testBspJobOutput");    	
       	hdfs.delete(inputPath, true);
    	hdfs.mkdirs(inputPath);
    	byte[] outputArray = new byte[20];
    	for (int i = 0; i < 20; ++i) {
    		if ((i % 2) == 1) {
    			outputArray[i] = 13;
    		}
    		else {
    			outputArray[i] = (byte) (65 + (i % 26));
    		}
    	}
    	FSDataOutputStream outputStream = 
    		hdfs.create(new Path(inputPath + "/testFile"), true);
		outputStream.write(outputArray);
		outputStream.close();
    	hdfs.delete(outputPath, true);
    	FileInputFormat.addInputPath(bspJob, inputPath);
    	FileOutputFormat.setOutputPath(bspJob, outputPath);
    	bspJob.run();
    }
    
    /**
     * Run a sample BSP job locally with multiple mappers.
     * @throws IOException
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public void tBspJobMultiple() 
    	throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 2);
        conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
        conf.setInt(BspJob.BSP_MIN_PROCESSES, 2);
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 1);
        conf.setInt(BspJob.BSP_POLL_MSECS, 1000);
        FileSystem hdfs = FileSystem.get(conf);
    	BspJob bspJob = new BspJob(conf, "testBspJob");
       	Path inputPath = new Path("/tmp/testBspJobMultipleInput");
       	Path outputPath = new Path("/tmp/testBspJobMultipleOutput");
       	hdfs.delete(inputPath, true);
    	hdfs.mkdirs(inputPath);
    	byte[] outputArray = new byte[20];
    	for (int i = 0; i < 20; ++i) {
    		if ((i % 2) == 1) {
    			outputArray[i] = 13;
    		}
    		else {
    			outputArray[i] = (byte) (65 + (i % 26));
    		}
    	}
    	FSDataOutputStream outputStream = 
    		hdfs.create(new Path(inputPath + "/testFile1"), true);
		outputStream.write(outputArray);
		outputStream.close();
		outputStream = 
    		hdfs.create(new Path(inputPath + "/testFile2"), true);
		outputStream.write(outputArray);
		outputStream.close();
    	hdfs.delete(outputPath, true);
    	FileInputFormat.addInputPath(bspJob, inputPath);
    	FileOutputFormat.setOutputPath(bspJob, outputPath);
    	bspJob.run();
    }
}
