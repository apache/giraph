package com.yahoo.hadoop_bsp;

import java.io.IOException;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.yahoo.hadoop_bsp.examples.TestCheckpointVertex;
import com.yahoo.hadoop_bsp.examples.TestVertexInputFormat;
import com.yahoo.hadoop_bsp.examples.TestVertexReader;
import com.yahoo.hadoop_bsp.examples.TestVertexWriter;
import com.yahoo.hadoop_bsp.lib.LongSumAggregator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for manual checkpoint restarting
 */
public class ManualCheckpointTest extends TestCase implements Watcher {
    /** JobTracker system property */
    private static String m_jobTracker =
        System.getProperty("prop.mapred.job.tracker");
    /** Jar location system property */
    private String m_jarLocation = System.getProperty("prop.jarLocation", "");
    /** Number of actual processes for the BSP application */
    private static int m_numProcs = 1;
    /** ZooKeeper list system property */
    private String m_zkList = System.getProperty("prop.zookeeper.list");

    /** Where the checkpoints will be stored and restarted */
    private final String HDFS_CHECKPOINT_DIR =
        "/tmp/testBspCheckpoints";

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ManualCheckpointTest(String testName) {
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
        return new TestSuite(ManualCheckpointTest.class);
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
     * Run a sample BSP job locally and test checkpointing.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspCheckpoint()
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
            System.out.println("testBspCheckpoint: Using local job runner with " +
                               "location " + m_jarLocation + "...");
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        }
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        conf.setBoolean(BspJob.BSP_KEEP_ZOOKEEPER_DATA, true);
        if (m_zkList != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, m_zkList);
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(TestVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      TestCheckpointVertex.class,
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
        conf.set(BspJob.BSP_CHECKPOINT_DIRECTORY,
                 HDFS_CHECKPOINT_DIR);
        BspJob bspJob = new BspJob(conf, "testBspCheckpoint");
        Path outputPath = new Path("/tmp/testBspCheckpointOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
        if (m_jobTracker == null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            assertTrue(fileStatusArr.length == 1);
            assertTrue(fileStatusArr[0].getLen() == 34);
            long idSum =
                ((LongWritable) BspJob.BspMapper.getAggregator(
                    LongSumAggregator.class.getName()).
                        getAggregatedValue()).get();
            System.out.println("testBspCheckpoint: idSum = " + idSum);
            assertTrue(idSum == (4*5/2)*7);
        }

        // Restart the test from superstep 3
        conf.setLong(BspJob.BSP_RESTART_SUPERSTEP, 3);
        System.out.println(
            "testBspCheckpoint: Restarting from superstep 3" +
            " with checkpoint path = " + HDFS_CHECKPOINT_DIR);
        BspJob bspRestartedJob = new BspJob(conf, "testBspCheckpointRestarted");
        outputPath = new Path("/tmp/testBspCheckpointRestartedOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspRestartedJob, outputPath);
        assertTrue(bspRestartedJob.run());
        if (m_jobTracker == null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            assertTrue(fileStatusArr.length == 1);
            assertTrue(fileStatusArr[0].getLen() == 34);
            long idSum =
                ((LongWritable) BspJob.BspMapper.getAggregator(
                    LongSumAggregator.class.getName()).
                        getAggregatedValue()).get();
            System.out.println("testBspCheckpoint: idSum = " + idSum);
            assertTrue(idSum == (4*5/2)*7);
        }
    }

    public void process(WatchedEvent event) {
        return;
    }
}
