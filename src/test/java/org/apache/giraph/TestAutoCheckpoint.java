/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

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

    
    public TestAutoCheckpoint() {
        super(TestAutoCheckpoint.class.getName());
    }

    /**
     * Run a job that requires checkpointing and will have a worker crash
     * and still recover from a previous checkpoint.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void testSingleFault()
            throws IOException, InterruptedException, ClassNotFoundException {
        if (getJobTracker() == null) {
            System.out.println(
                "testSingleFault: Ignore this test in local mode.");
            return;
        }
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.getConfiguration().setBoolean(SimpleCheckpointVertex.ENABLE_FAULT,
                                          true);
        job.getConfiguration().setInt("mapred.map.max.attempts", 4);
        job.getConfiguration().setInt(GiraphJob.POLL_MSECS, 5000);
        job.getConfiguration().set(GiraphJob.CHECKPOINT_DIRECTORY,
                                   HDFS_CHECKPOINT_DIR);
        job.getConfiguration().setBoolean(
            GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        job.setWorkerContextClass(
            SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
    }
}
