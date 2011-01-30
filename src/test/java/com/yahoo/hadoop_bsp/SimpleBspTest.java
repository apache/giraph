package com.yahoo.hadoop_bsp;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.yahoo.hadoop_bsp.examples.TestCombiner;
import com.yahoo.hadoop_bsp.examples.TestCombinerVertex;
import com.yahoo.hadoop_bsp.examples.TestFailVertex;
import com.yahoo.hadoop_bsp.examples.TestMsgVertex;
import com.yahoo.hadoop_bsp.examples.TestPageRankVertex;
import com.yahoo.hadoop_bsp.examples.TestSuperstepVertex;
import com.yahoo.hadoop_bsp.examples.TestVertexInputFormat;
import com.yahoo.hadoop_bsp.examples.TestVertexReader;
import com.yahoo.hadoop_bsp.examples.TestVertexWriter;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple BSP applications.
 */
public class SimpleBspTest extends TestCase implements Watcher {
    /** JobTracker system property */
    private static String m_jobTracker =
        System.getProperty("prop.mapred.job.tracker");
    /** Jar location system property */
    private String m_jarLocation = System.getProperty("prop.jarLocation", "");
    /** Number of actual processes for the BSP application */
    private static int m_numProcs = 1;
    /** ZooKeeper list system property */
    private String m_zkList = System.getProperty("prop.zookeeper.list");

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public SimpleBspTest(String testName) {
        super(testName);
        if (m_jobTracker != null) {
            System.out.println("Setting tasks to 3 for " + testName +
                               " since JobTracker exists...");
            setNumProcs(3);
        }
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(SimpleBspTest.class);
    }

    /**
     * Set the number of processes to use in the BSP application
     * @param numProcs number of processes to use
     */
    public static void setNumProcs(int numProcs) {
        m_numProcs = numProcs;
    }

    @Override
    public void setUp() {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            // Since local jobs always use the same paths, remove them
            Path oldLocalJobPaths = new Path(
                BspJob.DEFAULT_ZOOKEEPER_MANAGER_DIR);
            FileStatus [] fileStatusArr = hdfs.listStatus(oldLocalJobPaths);
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.isDir() &&
                        fileStatus.getPath().getName().contains("job_local")) {
                    System.out.println("Cleaning up local job path " +
                                       fileStatus.getPath().getName());
                    hdfs.delete(oldLocalJobPaths, true);
                }
            }
            if (m_zkList == null) {
                return;
            }
            ZooKeeperExt zooKeeperExt =
                new ZooKeeperExt(m_zkList, 30*1000, this);
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
        Configuration conf = new Configuration();
        List<InputSplit> splitArray = inputFormat.getSplits(conf, 1);
        ByteArrayOutputStream byteArrayOutputStream =
            new ByteArrayOutputStream();
        DataOutputStream outputStream =
            new DataOutputStream(byteArrayOutputStream);
        ((Writable) splitArray.get(0)).write(outputStream);
        System.out.println("testInstantiateVertex: Example output split = " +
                           byteArrayOutputStream.toString());
    }

    /**
     * Run a sample BSP job in JobTracker, kill a task, and make sure the job fails.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspFail()
        throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.jar", m_jarLocation);
        // Allow this test only to be run on a real Hadoop setup
        if (m_jobTracker == null) {
            System.out.println("testBspFail: not executed for local setup.");
            return;
        }
        System.out.println("testBspFail: Sending job to job tracker " +
                       m_jobTracker + " with jar path " + m_jarLocation);
        conf.set("mapred.job.tracker", m_jobTracker);
        conf.setInt(BspJob.BSP_INITIAL_PROCESSES, m_numProcs);
        conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
        conf.setInt(BspJob.BSP_MIN_PROCESSES, m_numProcs);
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        if (m_zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, m_zkList);
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(TestVertexReader.READER_VERTICES, 15);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      TestFailVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.BSP_INPUT_SPLIT_CLASS,
                      BspInputSplit.class,
                      InputSplit.class);
        conf.setClass(BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        BspJob bspJob = new BspJob(conf, "testBspFail");
        Path outputPath = new Path("/tmp/testBspFailOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(!bspJob.run());
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
        // Allow this test to be run on a real Hadoop setup
        conf.set("mapred.jar", m_jarLocation);

        if (m_jobTracker != null) {
              System.out.println("testBspSuperstep: Sending job to job tracker " +
                       m_jobTracker + " with jar path " + m_jarLocation);
            conf.set("mapred.job.tracker", m_jobTracker);
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, m_numProcs);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, m_numProcs);
        }
        else {
              System.out.println("testBspSuperStep: Using local job runner with " +
                               "location " + m_jarLocation + "...");
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        }

        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        if (m_zkList != null) {
              conf.set(BspJob.BSP_ZOOKEEPER_LIST, m_zkList);
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        conf.setFloat(BspJob.BSP_TOTAL_INPUT_SPLIT_MULTIPLIER, 2.0f);
        // GeneratedInputSplit will generate 10 vertices
        conf.setLong(TestVertexReader.READER_VERTICES, 10);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      TestSuperstepVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.BSP_INPUT_SPLIT_CLASS,
                      BspInputSplit.class,
                      InputSplit.class);
        conf.setClass(BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(BspJob.BSP_VERTEX_WRITER_CLASS,
                      TestVertexWriter.class,
                      VertexWriter.class);
        BspJob bspJob = new BspJob(conf, "testBspSuperStep");
        Path outputPath = new Path("/tmp/testBspSuperStepOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
        if (m_jobTracker == null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            assertTrue(fileStatusArr.length == 1);
            assertTrue(fileStatusArr[0].getLen() == 49);
        }
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
        conf.set("mapred.jar", m_jarLocation);
        // Allow this test to be run on a real Hadoop setup
        if (m_jobTracker != null) {
            System.out.println("testBspMsg: Sending job to job tracker " +
                       m_jobTracker + " with jar path " + m_jarLocation);
            conf.set("mapred.job.tracker", m_jobTracker);
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, m_numProcs);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, m_numProcs);
        }
        else {
            System.out.println("testBspMsg: Using local job runner with " +
                               "location " + m_jarLocation + "...");
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        }

        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        if (m_zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, m_zkList);
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      TestMsgVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.BSP_INPUT_SPLIT_CLASS,
                      BspInputSplit.class,
                      InputSplit.class);
        conf.setClass(BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        BspJob bspJob = new BspJob(conf, "testBspMsg");
        Path outputPath = new Path("/tmp/testBspMsgOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
    }

    /**
     * Run a sample BSP job locally with combiner and checkout output value.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspCombiner()
        throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.jar", m_jarLocation);
        // Allow this test to be run on a real Hadoop setup
        if (m_jobTracker != null) {
            System.out.println("testBspMsg: Sending job to job tracker " +
                       m_jobTracker + " with jar path " + m_jarLocation);
            conf.set("mapred.job.tracker", m_jobTracker);
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, m_numProcs);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, m_numProcs);
        }
        else {
            System.out.println("testBspMsg: Using local job runner with " +
                               "location " + m_jarLocation + "...");
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        }

        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        if (m_zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, m_zkList);
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      TestCombinerVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.BSP_INPUT_SPLIT_CLASS,
                      BspInputSplit.class,
                      InputSplit.class);
        conf.setClass(BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(BspJob.BSP_COMBINER_CLASS,
                      TestCombiner.class,
                      Combiner.class);
        BspJob bspJob = new BspJob(conf, "testBspCombiner");
        Path outputPath = new Path("/tmp/testBspCombinerOutput");
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
        conf.set("mapred.jar", m_jarLocation);
        // Allow this test to be run on a real Hadoop setup
        if (m_jobTracker != null) {
            System.out.println("testBspJob: Sending job to job tracker " +
                       m_jobTracker + " with jar path " + m_jarLocation);
            conf.set("mapred.job.tracker", m_jobTracker);
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, m_numProcs);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, m_numProcs);
        }
        else {
            System.out.println("testBspPageRank: Using local job runner with " +
                               "location " + m_jarLocation + "...");
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        }
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        if (m_zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, m_zkList);
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      TestPageRankVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.BSP_INPUT_SPLIT_CLASS,
                      BspInputSplit.class,
                      InputSplit.class);
        conf.setClass(BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                      TestVertexInputFormat.class,
                      VertexInputFormat.class);
        BspJob bspJob = new BspJob(conf, "testBspPageRank");
        Path outputPath = new Path("/tmp/testBspPageRankOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
        double maxPageRank = ((DoubleWritable)BspJob.BspMapper.
                getAggregator("max").getAggregatedValue()).get();
        double minPageRank = ((DoubleWritable)BspJob.BspMapper.
                getAggregator("min").getAggregatedValue()).get();
        long numVertices = ((LongWritable)BspJob.BspMapper.
                getAggregator("sum").getAggregatedValue()).get();
        System.out.println("testBspPageRank: maxPageRank=" + maxPageRank +
                           " minPageRank=" + minPageRank +
                           " numVertices=" + numVertices);
        assertTrue(maxPageRank > 0.19847 && maxPageRank < 0.19848);
        assertTrue(minPageRank > 0.03 && minPageRank < 0.03001);
        assertTrue(numVertices == 5);
    }

    public void process(WatchedEvent event) {
        return;
    }
}
