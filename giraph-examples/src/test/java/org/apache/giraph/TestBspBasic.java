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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.combiner.SimpleSumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimpleCombinerComputation;
import org.apache.giraph.examples.SimpleFailComputation;
import org.apache.giraph.examples.SimpleMasterComputeComputation;
import org.apache.giraph.examples.SimpleMsgComputation;
import org.apache.giraph.examples.SimplePageRankComputation;
import org.apache.giraph.examples.SimplePageRankComputation.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimpleShortestPathsComputation;
import org.apache.giraph.examples.SimpleSuperstepComputation;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepComputation.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.job.HadoopUtils;
import org.apache.giraph.master.input.LocalityAwareInputSplitsMasterOrganizer;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

/**
 * Unit test for many simple BSP applications.
 */
public class
    TestBspBasic extends BspCase {

  public TestBspBasic() {
    super(TestBspBasic.class.getName());
  }

  /**
   * Just instantiate the vertex (all functions are implemented) and the
   * VertexInputFormat using reflection.
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws InterruptedException
   * @throws IOException
   * @throws InvocationTargetException
   * @throws IllegalArgumentException
   * @throws NoSuchMethodException
   * @throws SecurityException
   */
  @Test
  public void testInstantiateVertex()
      throws InstantiationException, IllegalAccessException,
      IOException, InterruptedException, IllegalArgumentException,
      InvocationTargetException, SecurityException, NoSuchMethodException {
    System.out.println("testInstantiateVertex: java.class.path=" +
        System.getProperty("java.class.path"));
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleSuperstepComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    ImmutableClassesGiraphConfiguration configuration =
        new ImmutableClassesGiraphConfiguration(job.getConfiguration());
    Vertex<LongWritable, IntWritable, FloatWritable> vertex =
        configuration.createVertex();
    vertex.initialize(new LongWritable(1), new IntWritable(1));
    System.out.println("testInstantiateVertex: Got vertex " + vertex);
    VertexInputFormat<LongWritable, IntWritable, FloatWritable>
    inputFormat = configuration.createWrappedVertexInputFormat();
    List<InputSplit> splitArray = inputFormat.getSplits(
        HadoopUtils.makeJobContext(), 1);
    ByteArrayOutputStream byteArrayOutputStream =
        new ByteArrayOutputStream();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    ((Writable) splitArray.get(0)).write(outputStream);
    System.out.println("testInstantiateVertex: Example output split = " +
        byteArrayOutputStream.toString());
  }

  private static class NullComputation extends NoOpComputation<NullWritable,
      NullWritable, NullWritable, NullWritable> { }

  /**
   * Test whether vertices with NullWritable for vertex value type, edge value
   * type and message value type can be instantiated.
   */
  @Test
  public void testInstantiateNullVertex() throws IOException {
    GiraphConfiguration nullConf = new GiraphConfiguration();
    nullConf.setComputationClass(NullComputation.class);
    ImmutableClassesGiraphConfiguration<NullWritable, NullWritable,
        NullWritable> immutableClassesGiraphConfiguration =
        new ImmutableClassesGiraphConfiguration<
            NullWritable, NullWritable, NullWritable>(nullConf);
    NullWritable vertexValue =
        immutableClassesGiraphConfiguration.createVertexValue();
    NullWritable edgeValue =
        immutableClassesGiraphConfiguration.createEdgeValue();
    Writable messageValue =
        immutableClassesGiraphConfiguration.createOutgoingMessageValueFactory()
            .newInstance();
    assertSame(vertexValue.getClass(), NullWritable.class);
    assertSame(vertexValue, edgeValue);
    assertSame(edgeValue, messageValue);
  }

  /**
   * Do some checks for local job runner.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testLocalJobRunnerConfig()
      throws IOException, InterruptedException, ClassNotFoundException {
    if (runningInDistributedMode()) {
      System.out.println("testLocalJobRunnerConfig: Skipping for " +
          "non-local");
      return;
    }
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleSuperstepComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    conf = job.getConfiguration();
    conf.setWorkerConfiguration(5, 5, 100.0f);
    GiraphConstants.SPLIT_MASTER_WORKER.set(conf, true);

    try {
      job.run(true);
      fail();
    } catch (IllegalArgumentException e) {
    }

    GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
    try {
      job.run(true);
      fail();
    } catch (IllegalArgumentException e) {
    }

    conf.setWorkerConfiguration(1, 1, 100.0f);
    job.run(true);
  }

  /**
   * Run a sample BSP job in JobTracker, kill a task, and make sure
   * the job fails (not enough attempts to restart)
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspFail()
      throws IOException, InterruptedException, ClassNotFoundException {
    // Allow this test only to be run on a real Hadoop setup
    if (!runningInDistributedMode()) {
      System.out.println("testBspFail: not executed for local setup.");
      return;
    }

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleFailComputation.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf,
        getTempPath(getCallingMethodName()));
    job.getConfiguration().setInt("mapred.map.max.attempts", 1);
    assertTrue(!job.run(true));
  }

  /**
   * Run a sample BSP job locally and test supersteps.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspSuperStep()
      throws IOException, InterruptedException, ClassNotFoundException {
    String callingMethod = getCallingMethodName();
    Path outputPath = getTempPath(callingMethod);
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleSuperstepComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
    GiraphJob job = prepareJob(callingMethod, conf, outputPath);
    Configuration configuration = job.getConfiguration();
    // GeneratedInputSplit will generate 10 vertices
    GeneratedVertexReader.READER_VERTICES.set(configuration, 10);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      FileStatus fileStatus = getSinglePartFileStatus(configuration, outputPath);
      assertEquals(49l, fileStatus.getLen());
    }
  }

  /**
   * Run a sample BSP job locally and test messages.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspMsg()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleMsgComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
  }


  /**
   * Run a sample BSP job locally with no vertices and make sure
   * it completes.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testEmptyVertexInputFormat()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleMsgComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    GeneratedVertexReader.READER_VERTICES.set(job.getConfiguration(), 0);
    assertTrue(job.run(true));
  }

  /**
   * Run a sample BSP job locally with message combiner and
   * checkout output value.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspCombiner()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleCombinerComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    conf.setMessageCombinerClass(SimpleSumMessageCombiner.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
  }

  /**
   * Run a test to see if the InputSplitPathOrganizer can correctly sort
   * locality information from a mocked znode of data.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test
  public void testInputSplitLocality()
    throws IOException, KeeperException, InterruptedException {
    List<byte[]> serializedSplits = new ArrayList<>();
    serializedSplits.add(new byte[]{1});
    serializedSplits.add(new byte[]{2});
    serializedSplits.add(new byte[]{3});

    WorkerInfo workerInfo = mock(WorkerInfo.class);
    when(workerInfo.getTaskId()).thenReturn(5);
    when(workerInfo.getHostname()).thenReturn("node.LOCAL.com");

    List<InputSplit> splits = new ArrayList<>();
    InputSplit split1 = mock(InputSplit.class);
    when(split1.getLocations()).thenReturn(new String[]{
        "node.test1.com", "node.test2.com", "node.test3.com"});
    splits.add(split1);
    InputSplit split2 = mock(InputSplit.class);
    when(split2.getLocations()).thenReturn(new String[]{
        "node.testx.com", "node.LOCAL.com", "node.testy.com"});
    splits.add(split2);
    InputSplit split3 = mock(InputSplit.class);
    when(split3.getLocations()).thenReturn(new String[]{
        "node.test4.com", "node.test5.com", "node.test6.com"});
    splits.add(split3);

    LocalityAwareInputSplitsMasterOrganizer inputSplitOrganizer =
        new LocalityAwareInputSplitsMasterOrganizer(
            serializedSplits,
            splits,
            Lists.newArrayList(workerInfo));

    assertEquals(2,
        inputSplitOrganizer.getSerializedSplitFor(workerInfo.getTaskId())[0]);
  }

  /**
   * Run a sample BSP job locally and test shortest paths.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspShortestPaths()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath(getCallingMethodName());
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleShortestPathsComputation.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
        JsonLongDoubleFloatDoubleVertexOutputFormat.class);
    SimpleShortestPathsComputation.SOURCE_ID.set(conf, 0);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);

    assertTrue(job.run(true));

    int numResults = getNumResults(job.getConfiguration(), outputPath);

    int expectedNumResults = runningInDistributedMode() ? 15 : 5;
    assertEquals(expectedNumResults, numResults);
  }

  /**
   * Run a sample BSP job locally and test PageRank with AggregatorWriter.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspPageRankWithAggregatorWriter()
      throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath(getCallingMethodName());

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimplePageRankComputation.class);
    conf.setAggregatorWriterClass(TextAggregatorWriter.class);
    conf.setMasterComputeClass(
        SimplePageRankComputation.SimplePageRankMasterCompute.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
        SimplePageRankComputation.SimplePageRankVertexOutputFormat.class);
    conf.setWorkerContextClass(
        SimplePageRankComputation.SimplePageRankWorkerContext.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);
    GiraphConfiguration configuration = job.getConfiguration();
    Path aggregatorValues = getTempPath("aggregatorValues");
    configuration.setInt(TextAggregatorWriter.FREQUENCY,
        TextAggregatorWriter.ALWAYS);
    configuration.set(TextAggregatorWriter.FILENAME,
        aggregatorValues.toString());

    assertTrue(job.run(true));

    FileSystem fs = FileSystem.get(configuration);
    Path valuesFile = new Path(aggregatorValues.toString() + "_0");

    try {
      if (!runningInDistributedMode()) {
        double maxPageRank =
            SimplePageRankComputation.SimplePageRankWorkerContext.getFinalMax();
        double minPageRank =
            SimplePageRankComputation.SimplePageRankWorkerContext.getFinalMin();
        long numVertices =
            SimplePageRankComputation.SimplePageRankWorkerContext.getFinalSum();
        System.out.println("testBspPageRank: maxPageRank=" + maxPageRank +
            " minPageRank=" + minPageRank + " numVertices=" + numVertices);

        FSDataInputStream in = null;
        BufferedReader reader = null;
        try {
          Map<Integer, Double> minValues = Maps.newHashMap();
          Map<Integer, Double> maxValues = Maps.newHashMap();
          Map<Integer, Long> vertexCounts = Maps.newHashMap();

          in = fs.open(valuesFile);
          reader = new BufferedReader(new InputStreamReader(in,
              Charsets.UTF_8));
          String line;
          while ((line = reader.readLine()) != null) {
            String[] tokens = line.split("\t");
            int superstep = Integer.parseInt(tokens[0].split("=")[1]);
            String value = (tokens[1].split("=")[1]);
            String aggregatorName = (tokens[1].split("=")[0]);

            if ("min".equals(aggregatorName)) {
              minValues.put(superstep, Double.parseDouble(value));
            }
            if ("max".equals(aggregatorName)) {
              maxValues.put(superstep, Double.parseDouble(value));
            }
            if ("sum".equals(aggregatorName)) {
              vertexCounts.put(superstep, Long.parseLong(value));
            }
          }

          int maxSuperstep = SimplePageRankComputation.MAX_SUPERSTEPS;
          assertEquals(maxSuperstep + 2, minValues.size());
          assertEquals(maxSuperstep + 2, maxValues.size());
          assertEquals(maxSuperstep + 2, vertexCounts.size());

          assertEquals(maxPageRank, maxValues.get(maxSuperstep), 0d);
          assertEquals(minPageRank, minValues.get(maxSuperstep), 0d);
          assertEquals(numVertices, (long) vertexCounts.get(maxSuperstep));

        } finally {
          Closeables.close(in, true);
          Closeables.close(reader, true);
        }
      }
    } finally {
      fs.delete(valuesFile, false);
    }
  }

  /**
   * Run a sample BSP job locally and test MasterCompute.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testBspMasterCompute()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SimpleMasterComputeComputation.class);
    conf.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
    conf.setMasterComputeClass(
        SimpleMasterComputeComputation.SimpleMasterCompute.class);
    conf.setWorkerContextClass(
        SimpleMasterComputeComputation.SimpleMasterComputeWorkerContext.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
    if (!runningInDistributedMode()) {
      double finalSum =
          SimpleMasterComputeComputation.SimpleMasterComputeWorkerContext.getFinalSum();
      System.out.println("testBspMasterCompute: finalSum=" + finalSum);
      assertEquals(32.5, finalSum, 0d);
    }
  }

  /**
   * Test halting at superstep 0
   */
  @Test
  public void testHaltSuperstep0()
      throws IOException, InterruptedException, ClassNotFoundException {
    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphConstants.MAX_NUMBER_OF_SUPERSTEPS.set(conf, 0);
    conf.setComputationClass(SimpleMsgComputation.class);
    conf.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
    GiraphJob job = prepareJob(getCallingMethodName(), conf);
    assertTrue(job.run(true));
  }
}
