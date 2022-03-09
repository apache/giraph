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
import org.apache.giraph.examples.SimpleCheckpoint;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

/**
 * Unit test for not enough map tasks
 */
public class TestNotEnoughMapTasks extends BspCase {

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
    if (!runningInDistributedMode()) {
      System.out.println(
          "testNotEnoughMapTasks: Ignore this test in local mode.");
      return;
    }
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(
        SimpleCheckpoint.SimpleCheckpointComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);

    // An unlikely impossible number of workers to achieve
    final int unlikelyWorkers = Short.MAX_VALUE;
    job.getConfiguration().setWorkerConfiguration(unlikelyWorkers,
        unlikelyWorkers,
        100.0f);
    // Only one poll attempt of one second to make failure faster
    job.getConfiguration().setMaxMasterSuperstepWaitMsecs(1000);
    job.getConfiguration().setEventWaitMsecs(1000);
    assertFalse(job.run(false));
  }
}
