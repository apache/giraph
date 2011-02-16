package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yahoo.hadoop_bsp.examples.GeneratedVertexInputFormat;
import com.yahoo.hadoop_bsp.examples.GeneratedVertexReader;
import com.yahoo.hadoop_bsp.examples.SimpleCheckpointVertex;
import com.yahoo.hadoop_bsp.examples.SimpleVertexWriter;
import com.yahoo.hadoop_bsp.lib.LongSumAggregator;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for manual checkpoint restarting
 */
public class TestManualCheckpoint extends BspCase {
    /** Where the checkpoints will be stored and restarted */
    private final String HDFS_CHECKPOINT_DIR =
        "/tmp/testBspCheckpoints";

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestManualCheckpoint(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestManualCheckpoint.class);
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
        conf.set("mapred.jar", getJarLocation());
        // Allow this test to be run on a real Hadoop setup
        if (getJobTracker() != null) {
            System.out.println("testBspCheckpoint: Sending job to job tracker " +
                       getJarLocation() + " with jar path " + getJobTracker());
            conf.set("mapred.job.tracker", getJobTracker());
            conf.setInt(BspJob.BSP_INITIAL_PROCESSES, getNumWorkers());
            conf.setFloat(BspJob.BSP_MIN_PERCENT_RESPONDED, 100.0f);
            conf.setInt(BspJob.BSP_MIN_PROCESSES, getNumWorkers());
        }
        else {
            System.out.println("testBspCheckpoint: Using local job runner with " +
                               "location " + getJarLocation() + "...");
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
        conf.set(BspJob.BSP_CHECKPOINT_DIRECTORY,
                 HDFS_CHECKPOINT_DIR);
        BspJob bspJob = new BspJob(conf, "testBspCheckpoint");
        Path outputPath = new Path("/tmp/testBspCheckpointOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run());
        long fileLen = 0;
        long idSum = 0;
        if (getJobTracker() == null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            assertTrue(fileStatusArr.length == 1);
            fileLen = fileStatusArr[0].getLen();
            idSum =
                ((LongWritable) BspJob.BspMapper.getAggregator(
                    LongSumAggregator.class.getName()).
                        getAggregatedValue()).get();
            System.out.println("testBspCheckpoint: idSum = " + idSum +
                               " fileLen = " + fileLen);
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
        if (getJobTracker() == null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            assertTrue(fileStatusArr.length == 1);
            assertTrue(fileStatusArr[0].getLen() == fileLen);
            long idSumRestarted =
                ((LongWritable) BspJob.BspMapper.getAggregator(
                    LongSumAggregator.class.getName()).
                        getAggregatedValue()).get();
            System.out.println("testBspCheckpoint: idSumRestarted = " +
                               idSumRestarted);
            assertTrue(idSum == idSumRestarted);
        }
    }
}
