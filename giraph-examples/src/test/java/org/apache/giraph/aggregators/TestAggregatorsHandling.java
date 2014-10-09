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

package org.apache.giraph.aggregators;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.giraph.BspCase;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.examples.AggregatorsTestComputation;
import org.apache.giraph.examples.SimpleCheckpoint;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/** Tests if aggregators are handled on a proper way */
public class TestAggregatorsHandling extends BspCase {

  public TestAggregatorsHandling() {
    super(TestAggregatorsHandling.class.getName());
  }

  /** Tests if aggregators are handled on a proper way during supersteps */
  @Test
  public void testAggregatorsHandling() throws IOException,
      ClassNotFoundException, InterruptedException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(AggregatorsTestComputation.class);
    conf.setVertexInputFormatClass(
        AggregatorsTestComputation.SimpleVertexInputFormat.class);
    conf.setEdgeInputFormatClass(
        AggregatorsTestComputation.SimpleEdgeInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    job.getConfiguration().setMasterComputeClass(
        AggregatorsTestComputation.AggregatorsTestMasterCompute.class);
    // test with aggregators split in a few requests
    job.getConfiguration().setInt(
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST, 50);
    assertTrue(job.run(true));
  }

  /**
   * Test if aggregators are are handled properly when restarting from a
   * checkpoint
   */
  @Test
  public void testAggregatorsCheckpointing() throws ClassNotFoundException,
      IOException, InterruptedException {
    Path checkpointsDir = getTempPath("checkPointsForTesting");
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(AggregatorsTestComputation.class);
    conf.setMasterComputeClass(
        AggregatorsTestComputation.AggregatorsTestMasterCompute.class);
    conf.setVertexInputFormatClass(
        AggregatorsTestComputation.SimpleVertexInputFormat.class);
    conf.setEdgeInputFormatClass(
        AggregatorsTestComputation.SimpleEdgeInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);

    GiraphConfiguration configuration = job.getConfiguration();
    GiraphConstants.CHECKPOINT_DIRECTORY.set(configuration, checkpointsDir.toString());
    GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS.set(configuration, false);
    configuration.setCheckpointFrequency(4);

    assertTrue(job.run(true));

    // Restart the test from superstep 4
    System.out.println("testAggregatorsCheckpointing: Restarting from " +
        "superstep 4 with checkpoint path = " + checkpointsDir);
    outputPath = getTempPath(getCallingMethodName() + "Restarted");
    conf = new GiraphConfiguration();
    conf.setComputationClass(AggregatorsTestComputation.class);
    conf.setMasterComputeClass(
        AggregatorsTestComputation.AggregatorsTestMasterCompute.class);
    conf.setVertexInputFormatClass(
        AggregatorsTestComputation.SimpleVertexInputFormat.class);
    conf.setEdgeInputFormatClass(
        AggregatorsTestComputation.SimpleEdgeInputFormat.class);
    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        conf, outputPath);
    job.getConfiguration().setMasterComputeClass(
        SimpleCheckpoint.SimpleCheckpointVertexMasterCompute.class);
    GiraphConfiguration restartedJobConf = restartedJob.getConfiguration();
    GiraphConstants.CHECKPOINT_DIRECTORY.set(restartedJobConf,
        checkpointsDir.toString());
    restartedJobConf.setLong(GiraphConstants.RESTART_SUPERSTEP, 4);

    assertTrue(restartedJob.run(true));
  }
}
