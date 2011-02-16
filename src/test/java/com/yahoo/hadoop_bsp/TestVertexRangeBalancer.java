package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yahoo.hadoop_bsp.examples.GeneratedVertexInputFormat;
import com.yahoo.hadoop_bsp.examples.GeneratedVertexReader;
import com.yahoo.hadoop_bsp.examples.SimpleCheckpointVertex;
import com.yahoo.hadoop_bsp.examples.SimpleVertexWriter;
import com.yahoo.hadoop_bsp.examples.SuperstepBalancer;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for manual checkpoint restarting
 */
public class TestVertexRangeBalancer extends BspCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestVertexRangeBalancer(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestVertexRangeBalancer.class);
    }

    /**
     * Run a sample BSP job locally and test how the vertex ranges are sent
     * from one worker to another.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testSuperstepBalancer()
        throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.jar", getJarLocation());
        // Allow this test to be run on a real Hadoop setup
        if (getJobTracker() != null) {
            System.out.println("testSuperstepBalancer: Sending job to " +
                               "job tracker " + getJarLocation() +
                               " with jar path " + getJobTracker());
            conf.set("mapred.job.tracker", getJobTracker());
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, getNumWorkers());
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, getNumWorkers());
        }
        else {
            System.out.println("testSuperstepBalancer: Using local job " +
                               "runner with " + "location " +
                               getJarLocation() + "...");
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, 1);
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, 1);
        }
        conf.setInt(BspJob.BSP_POLL_ATTEMPTS, 5);
        conf.setInt(BspJob.BSP_POLL_MSECS, 3*1000);
        conf.setBoolean(BspJob.BSP_KEEP_ZOOKEEPER_DATA, true);
        if (getZooKeeperList() != null) {
            conf.set(BspJob.BSP_ZOOKEEPER_LIST, getZooKeeperList());
        }
        conf.setInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT);
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(GeneratedVertexReader.READER_VERTICES, 5);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.BSP_VERTEX_CLASS,
                      SimpleCheckpointVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.BSP_INPUT_SPLIT_CLASS,
                      BspInputSplit.class,
                      InputSplit.class);
        conf.setClass(BspJob.BSP_VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(BspJob.BSP_VERTEX_WRITER_CLASS,
                      SimpleVertexWriter.class,
                      VertexWriter.class);
        BspJob bspJob = new BspJob(conf, "testStaticBalancer");
        Path outputPath = new Path("/tmp/testStaticBalancer");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
           assertTrue(totalLen == 118);
        }

        conf.setClass(BspJob.BSP_VERTEX_RANGE_BALANCER_CLASS,
                      SuperstepBalancer.class,
                      VertexRangeBalancer.class);
        BspJob bspJob2 = new BspJob(conf, "testSuperstepBalancer");
        outputPath = new Path("/tmp/testSuperstepBalancer");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob2, outputPath);
        assertTrue(bspJob2.run());
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
            assertTrue(totalLen == 118);
        }
    }
}
