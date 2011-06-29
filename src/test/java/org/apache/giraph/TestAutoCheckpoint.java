package org.apache.giraph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.giraph.examples.GeneratedVertexInputFormat;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleVertexWriter;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for automated checkpoint restarting
 */
public class TestAutoCheckpoint extends BspCase {
    /** Where the checkpoints will be stored and restarted */
    private final String HDFS_CHECKPOINT_DIR =
        "/tmp/testBspCheckpoints";

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestAutoCheckpoint(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestAutoCheckpoint.class);
    }

    /**
     * Run a job that requires checkpointing and will have a worker crash
     * and still recover from a previous checkpoint.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testSingleFault()
            throws IOException, InterruptedException, ClassNotFoundException {
        if (getJobTracker() == null) {
            System.out.println(
                "testSingleFault: Ignore this test in local mode.");
            return;
        }
        Configuration conf = new Configuration();
        setupConfiguration(conf);
        conf.setBoolean(SimpleCheckpointVertex.ENABLE_FAULT, true);
        conf.setInt("mapred.map.max.attempts", 4);
        conf.setInt(BspJob.POLL_MSECS, 5000);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setClass(BspJob.VERTEX_CLASS,
                      SimpleCheckpointVertex.class,
                      HadoopVertex.class);
        conf.setClass(BspJob.VERTEX_INPUT_FORMAT_CLASS,
                      GeneratedVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(BspJob.VERTEX_WRITER_CLASS,
                      SimpleVertexWriter.class,
                      VertexWriter.class);
        conf.set(BspJob.CHECKPOINT_DIRECTORY,
                 HDFS_CHECKPOINT_DIR);
        conf.setBoolean(BspJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
        BspJob bspJob = new BspJob(conf, "testSingleFault");
        Path outputPath = new Path("/tmp/testSingleFault");
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(bspJob, outputPath);
        assertTrue(bspJob.run(true));
    }
}
