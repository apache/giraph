package org.apache.giraph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.giraph.examples.GeneratedVertexInputFormat;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleTextVertexOutputFormat;
import org.apache.giraph.examples.SuperstepBalancer;
import org.apache.giraph.graph.AutoBalancer;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.BasicVertexRangeBalancer;
import org.apache.giraph.graph.VertexOutputFormat;

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
        setupConfiguration(conf);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleCheckpointVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS,
                      SimpleTextVertexOutputFormat.class,
                      VertexOutputFormat.class);
        GiraphJob bspJob = new GiraphJob(conf, "testStaticBalancer");
        Path outputPath = new Path("/tmp/testStaticBalancer");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run(true));
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

        conf.setClass(GiraphJob.VERTEX_RANGE_BALANCER_CLASS,
                      SuperstepBalancer.class,
                      BasicVertexRangeBalancer.class);
        GiraphJob bspJob2 = new GiraphJob(conf, "testSuperstepBalancer");
        outputPath = new Path("/tmp/testSuperstepBalancer");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob2, outputPath);
        assertTrue(bspJob2.run(true));
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

        conf.setClass(GiraphJob.VERTEX_RANGE_BALANCER_CLASS,
                      AutoBalancer.class,
                      BasicVertexRangeBalancer.class);
        GiraphJob bspJob3 = new GiraphJob(conf, "testAutoBalancer");
        outputPath = new Path("/tmp/testAutoBalancer");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob3, outputPath);
        assertTrue(bspJob3.run(true));
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
