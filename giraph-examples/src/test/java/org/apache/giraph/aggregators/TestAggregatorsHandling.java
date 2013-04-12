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

import org.apache.giraph.BspCase;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.examples.AggregatorsTestVertex;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.master.MasterAggregatorHandler;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests if aggregators are handled on a proper way */
public class TestAggregatorsHandling extends BspCase {

  public TestAggregatorsHandling() {
    super(TestAggregatorsHandling.class.getName());
  }

  private Map<String, AggregatorWrapper<Writable>> getAggregatorMap
      (MasterAggregatorHandler aggregatorHandler) {
    try {
      Field aggregtorMapField = aggregatorHandler.getClass().getDeclaredField
          ("aggregatorMap");
      aggregtorMapField.setAccessible(true);
      return (Map<String, AggregatorWrapper<Writable>>)
          aggregtorMapField.get(aggregatorHandler);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Tests if aggregators are handled on a proper way during supersteps */
  @Test
  public void testAggregatorsHandling() throws IOException,
      ClassNotFoundException, InterruptedException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexClass(AggregatorsTestVertex.class);
    conf.setVertexInputFormatClass(
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    job.getConfiguration().setMasterComputeClass(
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class);
    // test with aggregators split in a few requests
    job.getConfiguration().setInt(
        AggregatorUtils.MAX_BYTES_PER_AGGREGATOR_REQUEST, 50);
    assertTrue(job.run(true));
  }

  /** Test if aggregators serialization captures everything */
  @Test
  public void testMasterAggregatorsSerialization() throws
      IllegalAccessException, InstantiationException, IOException {
    ImmutableClassesGiraphConfiguration conf =
        Mockito.mock(ImmutableClassesGiraphConfiguration.class);
    Mockito.when(conf.getAggregatorWriterClass()).thenReturn(
        TextAggregatorWriter.class);
    Progressable progressable = Mockito.mock(Progressable.class);
    MasterAggregatorHandler handler =
        new MasterAggregatorHandler(conf, progressable);

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
        getAggregatorMap(handler).values()) {
      aggregator.setPreviousAggregatedValue(
          aggregator.getCurrentAggregatedValue());
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    handler.write(new DataOutputStream(out));

    MasterAggregatorHandler restartedHandler =
        new MasterAggregatorHandler(conf, progressable);
    restartedHandler.readFields(
        new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(2, getAggregatorMap(restartedHandler).size());

    AggregatorWrapper<Writable> regularAgg =
        getAggregatorMap(restartedHandler).get(regularAggName);
    assertTrue(
        regularAgg.getAggregatorClass().equals(LongSumAggregator.class));
    assertEquals(regularValue, regularAgg.getPreviousAggregatedValue());
    assertEquals(regularValue,
        restartedHandler.<LongWritable>getAggregatedValue(regularAggName));
    assertFalse(regularAgg.isPersistent());

    AggregatorWrapper<Writable> persistentAgg =
        getAggregatorMap(restartedHandler).get(persistentAggName);
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
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexClass(AggregatorsTestVertex.class);
    conf.setMasterComputeClass(
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class);
    conf.setVertexInputFormatClass(
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
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
    conf.setVertexClass(AggregatorsTestVertex.class);
    conf.setMasterComputeClass(
        AggregatorsTestVertex.AggregatorsTestMasterCompute.class);
    conf.setVertexInputFormatClass(
        SimplePageRankVertex.SimplePageRankVertexInputFormat.class);
    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        conf, outputPath);
    job.getConfiguration().setMasterComputeClass(
        SimpleCheckpointVertex.SimpleCheckpointVertexMasterCompute.class);
    GiraphConfiguration restartedJobConf = restartedJob.getConfiguration();
    GiraphConstants.CHECKPOINT_DIRECTORY.set(restartedJobConf,
        checkpointsDir.toString());
    restartedJobConf.setLong(GiraphConstants.RESTART_SUPERSTEP, 4);

    assertTrue(restartedJob.run(true));
  }
}
