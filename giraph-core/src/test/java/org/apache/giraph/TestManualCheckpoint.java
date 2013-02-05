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

import org.apache.giraph.conf.GiraphClasses;
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
    GiraphClasses classes = new GiraphClasses();
    classes.setVertexClass(
        SimpleCheckpointVertex.SimpleCheckpointComputation.class);
    classes.setWorkerContextClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
    classes.setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    classes.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    classes.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), classes, outputPath);

    job.getConfiguration().set(GiraphConstants.CHECKPOINT_DIRECTORY,
        checkpointsDir.toString());
    job.getConfiguration().setBoolean(
        GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
    job.getConfiguration().setCheckpointFrequency(2);

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
    classes = new GiraphClasses();
    classes.setVertexClass(
        SimpleCheckpointVertex.SimpleCheckpointComputation.class);
    classes.setWorkerContextClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
    classes.setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    classes.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    classes.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        classes, outputPath);
    job.getConfiguration().setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    restartedJob.getConfiguration().set(GiraphConstants.CHECKPOINT_DIRECTORY,
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
