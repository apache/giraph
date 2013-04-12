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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexClass(
        SimpleCheckpointVertex.SimpleCheckpointComputation.class);
    conf.setWorkerContextClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
    conf.setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);

    GiraphConfiguration configuration = job.getConfiguration();
    GiraphConstants.CHECKPOINT_DIRECTORY.set(configuration, checkpointsDir.toString());
    GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS.set(configuration, false);
    configuration.setCheckpointFrequency(2);

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
    conf = new GiraphConfiguration();
    conf.setVertexClass(
        SimpleCheckpointVertex.SimpleCheckpointComputation.class);
    conf.setWorkerContextClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
    conf.setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        conf, outputPath);
    configuration.setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    GiraphConstants.CHECKPOINT_DIRECTORY.set(restartedJob.getConfiguration(),
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
