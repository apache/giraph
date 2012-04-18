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

import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit test for not enough map tasks
 */
public class TestNotEnoughMapTasks extends BspCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestNotEnoughMapTasks(String testName) {
        super(testName);
    }
    
    public TestNotEnoughMapTasks() {
        super(TestNotEnoughMapTasks.class.getName());
    }

    /**
     * This job should always fail gracefully with not enough map tasks.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void testNotEnoughMapTasks()
            throws IOException, InterruptedException, ClassNotFoundException {
        if (getJobTracker() == null) {
            System.out.println(
                "testNotEnoughMapTasks: Ignore this test in local mode.");
            return;
        }
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        // An unlikely impossible number of workers to achieve
        final int unlikelyWorkers = Short.MAX_VALUE;
        job.setWorkerConfiguration(
            unlikelyWorkers, unlikelyWorkers, 100.0f);
        // Only one poll attempt of one second to make failure faster
        job.getConfiguration().setInt(GiraphJob.POLL_ATTEMPTS, 1);
        job.getConfiguration().setInt(GiraphJob.POLL_MSECS, 1);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertFalse(job.run(false));
    }
}
