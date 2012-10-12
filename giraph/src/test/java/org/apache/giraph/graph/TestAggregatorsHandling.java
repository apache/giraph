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

package org.apache.giraph.graph;

import org.apache.giraph.BspCase;
import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.aggregators.DoubleOverwriteAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.examples.AggregatorsTestVertex;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Tests if aggregators are handled on a proper way */
public class TestAggregatorsHandling extends BspCase {

  public TestAggregatorsHandling() {
    super(TestAggregatorsHandling.class.getName());
  }

  /** Tests if aggregators are handled on a proper way during supersteps */
  @Test
  public void testAggregatorsHandling() throws IOException,
      ClassNotFoundException, InterruptedException {
    GiraphJob job = prepareJob(getCallingMethodName(),
        AggregatorsTestVertex.class,
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    job.getConfiguration().setMasterComputeClass(
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class);
    assertTrue(job.run(true));
  }

  /** Test if aggregators serialization captures everything */
  @Test
  public void testMasterAggregatorsSerialization() throws
      IllegalAccessException, InstantiationException, IOException {
    MasterAggregatorHandler handler =
        new MasterAggregatorHandler(new Configuration());

    String regularAggName = "regular";
    LongWritable regularValue = new LongWritable(5);
    handler.registerAggregator(regularAggName, LongSumAggregator.class);
    handler.setAggregatedValue(regularAggName, regularValue);

    String persistentAggName = "persistent";
    DoubleWritable persistentValue = new DoubleWritable(10.5);
    handler.registerPersistentAggregator(persistentAggName,
        DoubleOverwriteAggregator.class);
    handler.setAggregatedValue(persistentAggName, persistentValue);

    for (AggregatorWrapper<Writable> aggregator :
        handler.getAggregatorMap().values()) {
      aggregator.setPreviousAggregatedValue(
          aggregator.getCurrentAggregatedValue());
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    handler.write(new DataOutputStream(out));

    MasterAggregatorHandler restartedHandler =
        new MasterAggregatorHandler(new Configuration());
    restartedHandler.readFields(
        new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(2, restartedHandler.getAggregatorMap().size());

    AggregatorWrapper<Writable> regularAgg =
        restartedHandler.getAggregatorMap().get(regularAggName);
    assertTrue(
        regularAgg.getAggregatorClass().equals(LongSumAggregator.class));
    assertEquals(regularValue, regularAgg.getPreviousAggregatedValue());
    assertEquals(regularValue,
        restartedHandler.<LongWritable>getAggregatedValue(regularAggName));
    assertFalse(regularAgg.isPersistent());

    AggregatorWrapper<Writable> persistentAgg =
        restartedHandler.getAggregatorMap().get(persistentAggName);
    assertTrue(persistentAgg.getAggregatorClass().equals
        (DoubleOverwriteAggregator.class));
    assertEquals(persistentValue, persistentAgg.getPreviousAggregatedValue());
    assertEquals(persistentValue,
        restartedHandler.<LongWritable>getAggregatedValue(persistentAggName));
    assertTrue(persistentAgg.isPersistent());
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
    GiraphJob job = prepareJob(getCallingMethodName(),
        AggregatorsTestVertex.class,
        null,
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class,
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class,
        null,
        outputPath);

    job.getConfiguration().set(GiraphConfiguration.CHECKPOINT_DIRECTORY,
        checkpointsDir.toString());
    job.getConfiguration().setBoolean(
        GiraphConfiguration.CLEANUP_CHECKPOINTS_AFTER_SUCCESS, false);
    job.getConfiguration().setInt(GiraphConfiguration.CHECKPOINT_FREQUENCY, 4);

    assertTrue(job.run(true));

    // Restart the test from superstep 4
    System.out.println("testAggregatorsCheckpointing: Restarting from " +
        "superstep 4 with checkpoint path = " + checkpointsDir);
    outputPath = getTempPath(getCallingMethodName() + "Restarted");
    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        AggregatorsTestVertex.class,
        null,
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class,
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class,
        null,
        outputPath);
    job.getConfiguration().setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    restartedJob.getConfiguration().set(
        GiraphConfiguration.CHECKPOINT_DIRECTORY, checkpointsDir.toString());
    restartedJob.getConfiguration().setLong(
        GiraphConfiguration.RESTART_SUPERSTEP, 4);

    assertTrue(restartedJob.run(true));
  }
}
