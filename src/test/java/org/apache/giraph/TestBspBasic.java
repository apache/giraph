package org.apache.giraph;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.giraph.examples.GeneratedVertexInputFormat;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimpleCombinerVertex;
import org.apache.giraph.examples.SimpleFailVertex;
import org.apache.giraph.examples.SimpleMsgVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.examples.SimpleSumCombiner;
import org.apache.giraph.examples.SimpleSuperstepVertex;
import org.apache.giraph.examples.SimpleTextVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Combiner;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexOutputFormat;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for many simple BSP applications.
 */
public class TestBspBasic extends BspCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestBspBasic(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestBspBasic.class);
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
            SimpleSuperstepVertex.class.getConstructor();
        assertNotNull(ctor);
        SimpleSuperstepVertex test =
            (SimpleSuperstepVertex) ctor.newInstance();
        System.out.println("testInstantiateVertex: superstep=" +
                           test.getSuperstep());
        GeneratedVertexInputFormat inputFormat =
            GeneratedVertexInputFormat.class.newInstance();
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
     * Run a sample BSP job in JobTracker, kill a task, and make sure
     * the job fails (not enough attempts to restart)
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspFail()
            throws IOException, InterruptedException, ClassNotFoundException {
        // Allow this test only to be run on a real Hadoop setup
        if (getJobTracker() == null) {
            System.out.println("testBspFail: not executed for local setup.");
            return;
        }
        Configuration conf = new Configuration();
        setupConfiguration(conf);
        conf.setInt("mapred.map.max.attempts", 1);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleFailVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        GiraphJob bspJob = new GiraphJob(conf, "testBspFail");
        Path outputPath = new Path("/tmp/testBspFailOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(!bspJob.run(true));
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
        setupConfiguration(conf);
        conf.setFloat(GiraphJob.TOTAL_INPUT_SPLIT_MULTIPLIER, 2.0f);
        // GeneratedInputSplit will generate 10 vertices
        conf.setLong(GeneratedVertexReader.READER_VERTICES, 10);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleSuperstepVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS,
                      SimpleTextVertexOutputFormat.class,
                      VertexOutputFormat.class);
        GiraphJob bspJob = new GiraphJob(conf, "testBspSuperStep");
        Path outputPath = new Path("/tmp/testBspSuperStepOutput");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run(true));
        if (getJobTracker() == null) {
            FileStatus fileStatus = getSinglePartFileStatus(hdfs, outputPath);
            assertTrue(fileStatus.getLen() == 49);
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
        setupConfiguration(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleMsgVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        GiraphJob bspJob = new GiraphJob(conf, "testBspMsg");
        assertTrue(bspJob.run(true));
    }


    /**
     * Run a sample BSP job locally with no vertices and make sure
     * it completes.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testEmptyVertexInputFormat()
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        setupConfiguration(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleMsgVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setLong(GeneratedVertexReader.READER_VERTICES, 0);
        GiraphJob bspJob = new GiraphJob(conf, "testEmptyVertexInputFormat");
        assertTrue(bspJob.run(true));
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
        setupConfiguration(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleCombinerVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.COMBINER_CLASS,
                      SimpleSumCombiner.class,
                      Combiner.class);
        GiraphJob bspJob = new GiraphJob(conf, "testBspCombiner");
        assertTrue(bspJob.run(true));
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
        setupConfiguration(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimplePageRankVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        GiraphJob bspJob = new GiraphJob(conf, "testBspPageRank");
        assertTrue(bspJob.run(true));
        if (getJobTracker() == null) {
            double maxPageRank = SimplePageRankVertex.finalMax;
            double minPageRank = SimplePageRankVertex.finalMin;
            long numVertices = SimplePageRankVertex.finalSum;
            System.out.println("testBspPageRank: maxPageRank=" + maxPageRank +
                               " minPageRank=" + minPageRank +
                               " numVertices=" + numVertices);
            assertTrue(maxPageRank > 0.19847 && maxPageRank < 0.19848);
            assertTrue(minPageRank > 0.03 && minPageRank < 0.03001);
            assertTrue(numVertices == 5);
        }
    }
}
