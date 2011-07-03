package org.apache.giraph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.giraph.examples.GeneratedVertexInputFormat;
import org.apache.giraph.examples.SimpleMutateGraphVertex;
import org.apache.giraph.examples.SimpleTextVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexOutputFormat;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for graph mutation
 */
public class TestMutateGraphVertex extends BspCase {
    /** Where the checkpoints will be stored and restarted */
    private final String HDFS_CHECKPOINT_DIR =
        "/tmp/testBspCheckpoints";

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestMutateGraphVertex(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestMutateGraphVertex.class);
    }

    /**
     * Run a job that tests the various graph mutations that can occur
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testMutateGraph()
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        setupConfiguration(conf);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      SimpleMutateGraphVertex.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS,
                      SimpleTextVertexOutputFormat.class,
                      VertexOutputFormat.class);
        conf.set(GiraphJob.CHECKPOINT_DIRECTORY,
                 HDFS_CHECKPOINT_DIR);
        GiraphJob bspJob = new GiraphJob(conf, "testMutateGraph");
        Path outputPath = new Path("/tmp/testMutateGraph");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run(true));
    }
}
