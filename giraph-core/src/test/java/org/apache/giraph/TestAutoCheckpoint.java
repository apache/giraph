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
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

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

    GiraphConfiguration conf = job.getConfiguration();
    conf.setBoolean(SimpleCheckpointVertex.ENABLE_FAULT, true);
    conf.setInt("mapred.map.max.attempts", 4);
    // Trigger failure faster
    conf.setInt("mapred.task.timeout", 10000);
    conf.setMaxMasterSuperstepWaitMsecs(10000);
    conf.setEventWaitMsecs(1000);
    conf.setCheckpointFrequency(2);
    conf.set(GiraphConstants.CHECKPOINT_DIRECTORY,
        getTempPath("_singleFaultCheckpoints").toString());
    conf.setBoolean(GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
    conf.setInt(GiraphConstants.ZOOKEEPER_SESSION_TIMEOUT, 10000);
    conf.setInt(GiraphConstants.ZOOKEEPER_MIN_SESSION_TIMEOUT, 10000);

    assertTrue(job.run(true));
  }
}
