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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit test for automated checkpoint restarting
 */
public class TestAutoCheckpoint extends BspCase {

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
    if (!runningInDistributedMode()) {
      System.out.println(
          "testSingleFault: Ignore this test in local mode.");
      return;
    }
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleCheckpointVertex.class,
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class,
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class,
        SimpleSuperstepVertexInputFormat.class,
        SimpleSuperstepVertexOutputFormat.class,
        outputPath);

    Configuration conf = job.getConfiguration();
    conf.setBoolean(SimpleCheckpointVertex.ENABLE_FAULT, true);
    conf.setInt("mapred.map.max.attempts", 4);
    // Trigger failure faster
    conf.setInt("mapred.task.timeout", 30000);
    conf.setInt(GiraphJob.POLL_MSECS, 5000);
    conf.setInt(GiraphJob.CHECKPOINT_FREQUENCY, 2);
    conf.set(GiraphJob.CHECKPOINT_DIRECTORY,
        getTempPath("_singleFaultCheckpoints").toString());
    conf.setBoolean(GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);

    assertTrue(job.run(true));
  }
}
