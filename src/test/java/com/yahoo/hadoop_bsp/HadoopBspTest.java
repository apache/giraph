package com.yahoo.hadoop_bsp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

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
	public static final class TestBsp extends HadoopVertex<String, String, Integer, Integer> {
	    public void compute() {
	    	if (getSuperstep() > 3) {
	    		voteToHalt();
	        }
	    }
	}
	
	/**
	 * Generated vertex reader
	 */
	public static final class GeneratedVertexReader implements 
		VertexReader<String, Integer, Integer> {
		long m_recordsRead = 0;
		long m_totalRecords = 0;
		
		public void initialize(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException {
			Configuration configuration = context.getConfiguration();
				m_totalRecords = configuration.getLong("vertices", 10);
		}
		
		public boolean next(String vertexId, 
							Integer vertexValue,
							Integer edgeValue) throws IOException {
			if (m_totalRecords <= m_recordsRead) {
				return false;
			}
			vertexId = "id" + Long.toString(m_recordsRead);
			vertexValue = (int) m_recordsRead;
			edgeValue = (int) (m_recordsRead * 10);
			++m_recordsRead;
			return true;
		}

		public long getPos() throws IOException {
			return m_recordsRead;
		}

		public void close() throws IOException {
		}

		public float getProgress() throws IOException {
			return m_recordsRead * 100.0f / m_totalRecords;
		}
	}
	
	/**
	 * Generated vertex input format.
	 */
	public static final class GeneratedVertexInputFormat implements 
		VertexInputFormat<String, Integer, Integer> {
		
		public List<InputSplit> getSplits(int numSplits)
			throws IOException, InterruptedException {
	        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
	        for (int i = 0; i < numSplits; ++i) {
	        	inputSplitList.add(new BspInputSplit(Integer.toString(i)));
	        }
	        return inputSplitList;
		}
		
		public VertexReader<String, Integer, Integer> createRecordReader(
			InputSplit split, TaskAttemptContext context) 
			throws IOException {
			return new GeneratedVertexReader();
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
    	IOException, InterruptedException, IllegalArgumentException, InvocationTargetException, SecurityException, NoSuchMethodException {
    	java.lang.reflect.Constructor<?> ctor = TestBsp.class.getConstructor();
    	assertNotNull(ctor);
    	TestBsp test = (TestBsp) ctor.newInstance();
        System.out.println(test.getSuperstep());
        GeneratedVertexInputFormat inputFormat = 
        	GeneratedVertexInputFormat.class.newInstance();
        List<InputSplit> splitArray = inputFormat.getSplits(1);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = 
        	new DataOutputStream(byteArrayOutputStream);
        ((Writable) splitArray.get(0)).write(outputStream);
        System.out.println("Example output split = " + 
        	byteArrayOutputStream.toString());
    }
    
    @Override
    public void setUp() {
    	try {
			ZooKeeperExt zooKeeperExt = 
				new ZooKeeperExt("localhost:2181", 30*1000, this);
			List<String> rootChildren = zooKeeperExt.getChildren("/", false);
			for (String rootChild : rootChildren) {
				if (rootChild.startsWith("job_local_")) {
					System.out.println("Cleaning up /" + rootChild);
					zooKeeperExt.deleteExt("/" + rootChild, -1, true);
				}
			}
			zooKeeperExt.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
        conf.setInt(BspJob.BSP_POLL_MSECS, 5*1000);
        conf.set(BspJob.BSP_ZOOKEEPER_LIST, "localhost:2181");
        /* GeneratedInputSplit will generate 5 vertices */
        conf.setLong("vertices", 5);
        FileSystem hdfs = FileSystem.get(conf);
    	conf.setClass("bsp.vertexClass", TestBsp.class, HadoopVertex.class);
    	conf.setClass("bsp.inputSplitClass", 
    				  BspInputSplit.class, 
    				  InputSplit.class);
    	conf.setClass("bsp.vertexInputFormatClass", 
    				  GeneratedVertexInputFormat.class,
    				  VertexInputFormat.class);
    	BspJob<Integer, String, String> bspJob = 
    		new BspJob<Integer, String, String>(conf, "testBspJob");
       	Path outputPath = new Path("/tmp/testBspJobOutput");    	
    	hdfs.delete(outputPath, true);
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
    	BspJob<String, Integer, Integer> bspJob = 
    		new BspJob<String, Integer, Integer>(conf, "testBspJob");
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

	public void process(WatchedEvent event) {
		return;
	}
}
