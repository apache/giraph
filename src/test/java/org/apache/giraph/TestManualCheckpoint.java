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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Unit test for manual checkpoint restarting
 */
public class TestManualCheckpoint extends BspCase {

  public TestManualCheckpoint() {
    super(TestManualCheckpoint.class.getName());
  }

  /**
   * Run a sample BSP job locally and test checkpointing.
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspCheckpoint()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path checkpointsDir = getTempPath("checkPointsForTesting");
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphJob job = prepareJob(getCallingMethodName(),
        SimpleCheckpointVertex.class,
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class,
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class,
        SimpleSuperstepVertexInputFormat.class,
        SimpleSuperstepVertexOutputFormat.class, outputPath);

    job.getConfiguration().set(GiraphJob.CHECKPOINT_DIRECTORY,
        checkpointsDir.toString());
    job.getConfiguration().setBoolean(
        GiraphJob.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
    job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 2);

    assertTrue(job.run(true));

    long idSum = 0;
    if (!runningInDistributedMode()) {
      FileStatus fileStatus = getSinglePartFileStatus(job.getConfiguration(),
          outputPath);
      idSum = SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext
          .getFinalSum();
      System.out.println("testBspCheckpoint: idSum = " + idSum +
          " fileLen = " + fileStatus.getLen());
    }

    // Restart the test from superstep 2
    System.out.println("testBspCheckpoint: Restarting from superstep 2" +
        " with checkpoint path = " + checkpointsDir);
    outputPath = getTempPath(getCallingMethodName() + "Restarted");
    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        SimpleCheckpointVertex.class,
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class,
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class,
        SimpleSuperstepVertexInputFormat.class,
        SimpleSuperstepVertexOutputFormat.class, outputPath);
    job.setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    restartedJob.getConfiguration().set(GiraphJob.CHECKPOINT_DIRECTORY,
        checkpointsDir.toString());

    assertTrue(restartedJob.run(true));
    if (!runningInDistributedMode()) {
      long idSumRestarted =
          SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext
              .getFinalSum();
      System.out.println("testBspCheckpoint: idSumRestarted = " +
          idSumRestarted);
      assertEquals(idSum, idSumRestarted);
    }
  }
}
