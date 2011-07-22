/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import java.io.IOException;

import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.getConfiguration().set(GiraphJob.CHECKPOINT_DIRECTORY,
                                   HDFS_CHECKPOINT_DIR);
        job.getConfiguration().setBoolean(
            GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        long fileLen = 0;
        long idSum = 0;
        if (getJobTracker() == null) {
            FileStatus fileStatus = getSinglePartFileStatus(job, outputPath);
            fileLen = fileStatus.getLen();
            idSum = SimpleCheckpointVertex.finalSum;
            System.out.println("testBspCheckpoint: idSum = " + idSum +
                               " fileLen = " + fileLen);
        }

        // Restart the test from superstep 2
        System.out.println(
            "testBspCheckpoint: Restarting from superstep 2" +
            " with checkpoint path = " + HDFS_CHECKPOINT_DIR);
        GiraphJob restartedJob = new GiraphJob(getCallingMethodName() +
                                               "Restarted");
        setupConfiguration(restartedJob);
        restartedJob.getConfiguration().set(GiraphJob.CHECKPOINT_DIRECTORY,
                                            HDFS_CHECKPOINT_DIR);
        restartedJob.getConfiguration().setLong(GiraphJob.RESTART_SUPERSTEP, 2);
        restartedJob.setVertexClass(SimpleCheckpointVertex.class);
        restartedJob.setVertexInputFormatClass(
            SimpleSuperstepVertexInputFormat.class);
        restartedJob.setVertexOutputFormatClass(
            SimpleSuperstepVertexOutputFormat.class);
        outputPath = new Path("/tmp/" + getCallingMethodName() + "Restarted");
        removeAndSetOutput(restartedJob, outputPath);
        assertTrue(restartedJob.run(true));
        if (getJobTracker() == null) {
            FileStatus fileStatus = getSinglePartFileStatus(job, outputPath);
            fileLen = fileStatus.getLen();
            assertTrue(fileStatus.getLen() == fileLen);
            long idSumRestarted = SimpleCheckpointVertex.finalSum;
            System.out.println("testBspCheckpoint: idSumRestarted = " +
                               idSumRestarted);
            assertTrue(idSum == idSumRestarted);
        }
    }
}
