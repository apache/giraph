package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.yahoo.hadoop_bsp.examples.GeneratedVertexInputFormat;
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
        setupConfiguration(conf);
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
