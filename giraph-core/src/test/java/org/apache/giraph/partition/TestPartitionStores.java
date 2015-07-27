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

package org.apache.giraph.partition;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
import org.apache.giraph.ooc.DiskBackedPartitionStore;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test case for partition stores.
 */
@SuppressWarnings("unchecked")
public class TestPartitionStores {
  private ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
      NullWritable> conf;
  private Mapper<?, ?, ?, ?>.Context context;

  /* these static variables are used for the multithreaded tests */
  private static final int NUM_OF_VERTEXES_PER_PARTITION = 20;
  private static final int NUM_OF_EDGES_PER_VERTEX = 5;
  private static final int NUM_OF_THREADS = 8;
  private static final int NUM_OF_PARTITIONS = 30;
  private static final int NUM_PARTITIONS_IN_MEMORY = 12;

  public static class MyComputation extends NoOpComputation<IntWritable,
      IntWritable, NullWritable, IntWritable> { }

  private Partition<IntWritable, IntWritable, NullWritable> createPartition(
      ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
          NullWritable> conf,
      Integer id,
      Vertex<IntWritable, IntWritable, NullWritable>... vertices) {
    Partition<IntWritable, IntWritable, NullWritable> partition =
        conf.createPartition(id, context);
    for (Vertex<IntWritable, IntWritable, NullWritable> v : vertices) {
      partition.putVertex(v);
    }
    return partition;
  }

  @Before
  public void setUp() {
    GiraphConfiguration configuration = new GiraphConfiguration();
    configuration.setComputationClass(MyComputation.class);
    conf = new ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
        NullWritable>(configuration);
    context = Mockito.mock(Mapper.Context.class);
  }

  @Test
  public void testSimplePartitionStore() {
    CentralizedServiceWorker<IntWritable, IntWritable, NullWritable>
    serviceWorker = Mockito.mock(CentralizedServiceWorker.class);
    PartitionStore<IntWritable, IntWritable, NullWritable>
    partitionStore = new SimplePartitionStore<IntWritable, IntWritable,
                NullWritable>(conf, context, serviceWorker);
    testReadWrite(partitionStore, conf);
    partitionStore.shutdown();
  }

  @Test
  public void testUnsafePartitionSerializationClass() throws IOException {
    conf.setPartitionClass(ByteArrayPartition.class);
    Vertex<IntWritable, IntWritable, NullWritable> v1 =
        conf.createVertex();
    v1.initialize(new IntWritable(1), new IntWritable(1));
    Vertex<IntWritable, IntWritable, NullWritable> v2 = conf.createVertex();
    v2.initialize(new IntWritable(2), new IntWritable(2));
    Vertex<IntWritable, IntWritable, NullWritable> v3 = conf.createVertex();
    v3.initialize(new IntWritable(3), new IntWritable(3));
    Vertex<IntWritable, IntWritable, NullWritable> v4 = conf.createVertex();
    v4.initialize(new IntWritable(4), new IntWritable(4));
    Vertex<IntWritable, IntWritable, NullWritable> v5 = conf.createVertex();
    v5.initialize(new IntWritable(5), new IntWritable(5));
    Vertex<IntWritable, IntWritable, NullWritable> v6 = conf.createVertex();
    v6.initialize(new IntWritable(6), new IntWritable(6));
    Vertex<IntWritable, IntWritable, NullWritable> v7 = conf.createVertex();
    v7.initialize(new IntWritable(7), new IntWritable(7));

    Partition<IntWritable, IntWritable, NullWritable> partition =
        createPartition(conf, 3, v1, v2, v3, v4, v5, v6, v7);
    assertEquals(3, partition.getId());
    assertEquals(0, partition.getEdgeCount());
    assertEquals(7, partition.getVertexCount());
    UnsafeByteArrayOutputStream outputStream = new
        UnsafeByteArrayOutputStream();
    partition.write(outputStream);
    UnsafeByteArrayInputStream inputStream = new UnsafeByteArrayInputStream(
        outputStream.getByteArray(), 0, outputStream.getPos());
    Partition<IntWritable, IntWritable, NullWritable> deserializatedPartition =
        conf.createPartition(-1, context);
    deserializatedPartition.readFields(inputStream);

    assertEquals(3, deserializatedPartition.getId());
    assertEquals(0, deserializatedPartition.getEdgeCount());
    assertEquals(7, deserializatedPartition.getVertexCount());
  }

  @Test
  public void testDiskBackedPartitionStoreWithByteArrayPartition()
    throws IOException {
    File directory = Files.createTempDir();
    GiraphConstants.PARTITIONS_DIRECTORY.set(
        conf, new File(directory, "giraph_partitions").toString());
    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, 1);
    conf.setPartitionClass(ByteArrayPartition.class);

    CentralizedServiceWorker<IntWritable, IntWritable, NullWritable>
      serviceWorker = Mockito.mock(CentralizedServiceWorker.class);
    Mockito.when(serviceWorker.getSuperstep()).thenReturn(
        BspService.INPUT_SUPERSTEP);

    PartitionStore<IntWritable, IntWritable, NullWritable> partitionStore =
        new DiskBackedPartitionStore<IntWritable, IntWritable, NullWritable>(
            conf, context, serviceWorker);
    testReadWrite(partitionStore, conf);
    partitionStore.shutdown();
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testDiskBackedPartitionStore() throws IOException {
    File directory = Files.createTempDir();
    GiraphConstants.PARTITIONS_DIRECTORY.set(
        conf, new File(directory, "giraph_partitions").toString());
    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, 1);

    CentralizedServiceWorker<IntWritable, IntWritable, NullWritable>
    serviceWorker = Mockito.mock(CentralizedServiceWorker.class);

    Mockito.when(serviceWorker.getSuperstep()).thenReturn(
        BspService.INPUT_SUPERSTEP);

    PartitionStore<IntWritable, IntWritable, NullWritable> partitionStore =
        new DiskBackedPartitionStore<IntWritable, IntWritable, NullWritable>(
            conf, context, serviceWorker);
    testReadWrite(partitionStore, conf);
    partitionStore.shutdown();

    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testDiskBackedPartitionStoreComputation() throws Exception {
    Iterable<String> results;
    String[] graph =
        {
            "[1,0,[]]", "[2,0,[]]", "[3,0,[]]", "[4,0,[]]", "[5,0,[]]",
            "[6,0,[]]", "[7,0,[]]", "[8,0,[]]", "[9,0,[]]", "[10,0,[]]"
        };
    String[] expected =
        {
            "1\t0", "2\t0", "3\t0", "4\t0", "5\t0",
            "6\t0", "7\t0", "8\t0", "9\t0", "10\t0"
        };

    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, 1);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, 10);

    File directory = Files.createTempDir();
    GiraphConstants.PARTITIONS_DIRECTORY.set(conf,
        new File(directory, "giraph_partitions").toString());

    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

    results = InternalVertexRunner.run(conf, graph);
    checkResults(results, expected);
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testDiskBackedPartitionStoreWithByteArrayComputation()
    throws Exception {
    Iterable<String> results;
    String[] graph =
    {
      "[1,0,[]]", "[2,0,[]]", "[3,0,[]]", "[4,0,[]]", "[5,0,[]]",
      "[6,0,[]]", "[7,0,[]]", "[8,0,[]]", "[9,0,[]]", "[10,0,[]]"
    };
    String[] expected =
    {
      "1\t0", "2\t0", "3\t0", "4\t0", "5\t0",
      "6\t0", "7\t0", "8\t0", "9\t0", "10\t0"
    };

    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, 1);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, 10);

    File directory = Files.createTempDir();
    GiraphConstants.PARTITIONS_DIRECTORY.set(conf,
      new File(directory, "giraph_partitions").toString());

    conf.setPartitionClass(ByteArrayPartition.class);
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

    results = InternalVertexRunner.run(conf, graph);
    checkResults(results, expected);
    FileUtils.deleteDirectory(directory);
  }

  @Test
  public void testDiskBackedPartitionStoreMT() throws Exception {
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, NUM_PARTITIONS_IN_MEMORY);
    GiraphConstants.STATIC_GRAPH.set(conf, false);
    testMultiThreaded();
  }


  @Test
  public void testDiskBackedPartitionStoreMTStatic() throws Exception {
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, NUM_PARTITIONS_IN_MEMORY);
    GiraphConstants.STATIC_GRAPH.set(conf, true);
    testMultiThreaded();
  }

  @Test
  public void testDiskBackedPartitionStoreAdaptiveOOC() throws Exception {
    GiraphConstants.STATIC_GRAPH.set(conf, true);
    testMultiThreaded();
  }

  private void testMultiThreaded() throws Exception {
    final AtomicInteger vertexCounter = new AtomicInteger(0);
    ExecutorService pool = Executors.newFixedThreadPool(NUM_OF_THREADS);
    ExecutorCompletionService<Boolean> executor =
      new ExecutorCompletionService<Boolean>(pool);

    File directory = Files.createTempDir();
    GiraphConstants.PARTITIONS_DIRECTORY.set(
        conf, new File(directory, "giraph_partitions").toString());
    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);

    CentralizedServiceWorker<IntWritable, IntWritable, NullWritable>
    serviceWorker = Mockito.mock(CentralizedServiceWorker.class);

    Mockito.when(serviceWorker.getSuperstep()).thenReturn(
        BspService.INPUT_SUPERSTEP);

    PartitionStore<IntWritable, IntWritable, NullWritable> store =
        new DiskBackedPartitionStore<IntWritable, IntWritable, NullWritable>(
            conf, context, serviceWorker);
    store.initialize();

    // Create a new Graph in memory using multiple threads
    for (int i = 0; i < NUM_OF_THREADS; ++i) {
      List<Integer> partitionIds = new ArrayList<Integer>();
      for (int id = i; id < NUM_OF_PARTITIONS; id += NUM_OF_THREADS) {
        partitionIds.add(id);
      }
      Worker worker =
        new Worker(vertexCounter, store, partitionIds, conf);
      executor.submit(worker, new Boolean(true));
    }
    for (int i = 0; i < NUM_OF_THREADS; ++i)
      executor.take();
    pool.shutdownNow();

    // Check the number of vertices
    int totalVertexes = 0;
    int totalEdges = 0;
    Partition<IntWritable, IntWritable, NullWritable> partition;
    for (int i = 0; i < NUM_OF_PARTITIONS; ++i) {
      totalVertexes += store.getPartitionVertexCount(i);
      totalEdges += store.getPartitionEdgeCount(i);
    }

    assert vertexCounter.get() == NUM_OF_PARTITIONS * NUM_OF_VERTEXES_PER_PARTITION;
    assert totalVertexes == NUM_OF_PARTITIONS * NUM_OF_VERTEXES_PER_PARTITION;
    assert totalEdges == totalVertexes * NUM_OF_EDGES_PER_VERTEX;

    // Check the content of the vertices
    int expected = 0;
    for (int i = 0; i < NUM_OF_VERTEXES_PER_PARTITION * NUM_OF_PARTITIONS; ++i) {
      expected += i;
    }
    int totalValues = 0;
    store.startIteration();
    for (int i = 0; i < NUM_OF_PARTITIONS; ++i) {
      partition = store.getNextPartition();
      assert partition != null;
      Iterator<Vertex<IntWritable, IntWritable, NullWritable>> vertexes =
        partition.iterator();

      while (vertexes.hasNext()) {
        Vertex<IntWritable, IntWritable, NullWritable> v = vertexes.next();
        totalValues += v.getId().get();
      }
      store.putPartition(partition);
    }
    assert totalValues == expected;

    store.shutdown();
  }

  private Partition<IntWritable, IntWritable, NullWritable>
  getPartition(PartitionStore<IntWritable, IntWritable,
      NullWritable> partitionStore, int partitionId) {
    partitionStore.startIteration();
    Partition p;
    Partition result = null;
    while ((p = partitionStore.getNextPartition()) != null) {
      if (p.getId() == partitionId) {
        result = p;
      }
      partitionStore.putPartition(p);
    }
    return result;
  }

  /**
   * Test reading/writing to/from a partition store
   *
   * @param partitionStore Partition store to test
   * @param conf Configuration to use
   */
  public void testReadWrite(
      PartitionStore<IntWritable, IntWritable,
          NullWritable> partitionStore,
      ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
          NullWritable> conf) {
    Vertex<IntWritable, IntWritable, NullWritable> v1 = conf.createVertex();
    v1.initialize(new IntWritable(1), new IntWritable(1));
    Vertex<IntWritable, IntWritable, NullWritable> v2 = conf.createVertex();
    v2.initialize(new IntWritable(2), new IntWritable(2));
    Vertex<IntWritable, IntWritable, NullWritable> v3 = conf.createVertex();
    v3.initialize(new IntWritable(3), new IntWritable(3));
    Vertex<IntWritable, IntWritable, NullWritable> v4 = conf.createVertex();
    v4.initialize(new IntWritable(4), new IntWritable(4));
    Vertex<IntWritable, IntWritable, NullWritable> v5 = conf.createVertex();
    v5.initialize(new IntWritable(5), new IntWritable(5));
    Vertex<IntWritable, IntWritable, NullWritable> v6 = conf.createVertex();
    v6.initialize(new IntWritable(7), new IntWritable(7));
    Vertex<IntWritable, IntWritable, NullWritable> v7 = conf.createVertex();
    v7.initialize(new IntWritable(7), new IntWritable(7));
    v7.addEdge(EdgeFactory.create(new IntWritable(1)));
    v7.addEdge(EdgeFactory.create(new IntWritable(2)));

    partitionStore.addPartition(createPartition(conf, 1, v1, v2, v6));
    partitionStore.addPartition(createPartition(conf, 2, v3, v4));
    partitionStore.addPartition(createPartition(conf, 3, v5));
    partitionStore.addPartition(createPartition(conf, 4, v7));

    getPartition(partitionStore, 1);
    getPartition(partitionStore, 2);
    partitionStore.removePartition(3);
    getPartition(partitionStore, 4);

    assertEquals(3, partitionStore.getNumPartitions());
    assertEquals(3, Iterables.size(partitionStore.getPartitionIds()));
    int partitionsNumber = 0;

    partitionStore.startIteration();
    Partition<IntWritable, IntWritable, NullWritable> p;
    while ((p = partitionStore.getNextPartition()) != null) {
      partitionStore.putPartition(p);
      partitionsNumber++;
    }
    assertEquals(3, partitionsNumber);
    assertTrue(partitionStore.hasPartition(1));
    assertTrue(partitionStore.hasPartition(2));
    assertFalse(partitionStore.hasPartition(3));
    assertTrue(partitionStore.hasPartition(4));
    assertEquals(3, partitionStore.getPartitionVertexCount(1));
    assertEquals(2, partitionStore.getPartitionVertexCount(2));
    assertEquals(1, partitionStore.getPartitionVertexCount(4));
    assertEquals(2, partitionStore.getPartitionEdgeCount(4));
  }

  /**
   * Internal checker to verify the correctness of the tests.
   * @param results   the actual results obtaind
   * @param expected  expected results
   */
  private void checkResults(Iterable<String> results, String[] expected) {
    Iterator<String> result = results.iterator();

    assert results != null;

    while(result.hasNext()) {
      String  resultStr = result.next();
      boolean found = false;

      for (int j = 0; j < expected.length; ++j) {
        if (expected[j].equals(resultStr)) {
          found = true;
        }
      }

      assert found;
    }
  }

  /**
   * Test compute method that sends each edge a notification of its parents.
   * The test set only has a 1-1 parent-to-child ratio for this unit test.
   */
  public static class EmptyComputation
    extends BasicComputation<LongWritable, DoubleWritable, FloatWritable,
      LongWritable> {

    @Override
    public void compute(
      Vertex<LongWritable, DoubleWritable,FloatWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {

      vertex.voteToHalt();
    }
  }

  @Test
  public void testEdgeCombineWithSimplePartition() throws IOException {
    testEdgeCombine(SimplePartition.class);
  }

  @Test
  public void testEdgeCombineWithByteArrayPartition() throws IOException {
    testEdgeCombine(ByteArrayPartition.class);
  }

  private void testEdgeCombine(Class<? extends Partition> partitionClass)
      throws IOException {
    Vertex<IntWritable, IntWritable, NullWritable> v1 = conf.createVertex();
    v1.initialize(new IntWritable(1), new IntWritable(1));
    Vertex<IntWritable, IntWritable, NullWritable> v2 = conf.createVertex();
    v2.initialize(new IntWritable(2), new IntWritable(2));
    Vertex<IntWritable, IntWritable, NullWritable> v3 = conf.createVertex();
    v3.initialize(new IntWritable(3), new IntWritable(3));
    Vertex<IntWritable, IntWritable, NullWritable> v1e2 = conf.createVertex();
    v1e2.initialize(new IntWritable(1), new IntWritable(1));
    v1e2.addEdge(EdgeFactory.create(new IntWritable(2)));
    Vertex<IntWritable, IntWritable, NullWritable> v1e3 = conf.createVertex();
    v1e3.initialize(new IntWritable(1), new IntWritable(1));
    v1e3.addEdge(EdgeFactory.create(new IntWritable(3)));

    GiraphConfiguration newconf = new GiraphConfiguration(conf);
    newconf.setPartitionClass(partitionClass);
    Partition<IntWritable, IntWritable, NullWritable> partition =
        (new ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
            NullWritable>(newconf)).createPartition(1, context);
    assertEquals(partitionClass, partition.getClass());
    partition.putVertex(v1);
    partition.putVertex(v2);
    partition.putVertex(v3);
    assertEquals(3, partition.getVertexCount());
    assertEquals(0, partition.getEdgeCount());
    partition.putOrCombine(v1e2);
    assertEquals(3, partition.getVertexCount());
    assertEquals(1, partition.getEdgeCount());
    partition.putOrCombine(v1e3);
    assertEquals(3, partition.getVertexCount());
    assertEquals(2, partition.getEdgeCount());
    v1 = partition.getVertex(new IntWritable(1));
    assertEquals(new IntWritable(1), v1.getId());
    assertEquals(new IntWritable(1), v1.getValue());
    assertEquals(2, v1.getNumEdges());
  }

  private class Worker implements Runnable {

    private final AtomicInteger vertexCounter;
    private final PartitionStore<IntWritable, IntWritable, NullWritable>
      partitionStore;
    private final List<Integer> partitionIds;
    private final ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
            NullWritable> conf;

    public Worker(AtomicInteger vertexCounter,
        PartitionStore<IntWritable, IntWritable, NullWritable> partitionStore,
        List<Integer> partitionIds,
        ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
          NullWritable> conf) {

      this.vertexCounter = vertexCounter;
      this.partitionStore = partitionStore;
      this.partitionIds = partitionIds;
      this.conf = conf;
    }

    public void run() {
      for (int partitionId : partitionIds) {
        Partition<IntWritable, IntWritable, NullWritable> partition =
            conf.createPartition(partitionId, context);
        for (int i = 0; i < NUM_OF_VERTEXES_PER_PARTITION; ++i) {
          int id = vertexCounter.getAndIncrement();
          Vertex<IntWritable, IntWritable, NullWritable> v = conf.createVertex();
          v.initialize(new IntWritable(id), new IntWritable(id));

          Random rand = new Random(id);
          for (int j = 0; j < NUM_OF_EDGES_PER_VERTEX; ++j) {
            int dest = rand.nextInt(id + 1);
            v.addEdge(EdgeFactory.create(new IntWritable(dest)));
          }

          partition.putVertex(v);
        }
        partitionStore.addPartition(partition);
      }
    }
  }
}
