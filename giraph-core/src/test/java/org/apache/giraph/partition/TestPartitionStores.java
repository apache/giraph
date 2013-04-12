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

import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test case for partition stores.
 */
public class TestPartitionStores {
  private ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
      NullWritable, IntWritable> conf;
  private Mapper<?, ?, ?, ?>.Context context;

  public static class MyVertex extends Vertex<IntWritable, IntWritable,
      NullWritable, IntWritable> {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {}
  }

  private Partition<IntWritable, IntWritable, NullWritable,
        IntWritable> createPartition(
      ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
          NullWritable, IntWritable> conf,
      Integer id,
      Vertex<IntWritable, IntWritable, NullWritable,
          IntWritable>... vertices) {
    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition =
        conf.createPartition(id, context);
    for (Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v :
        vertices) {
      partition.putVertex(v);
    }
    return partition;
  }

  @Before
  public void setUp() {
    GiraphConfiguration configuration = new GiraphConfiguration();
    configuration.setVertexClass(MyVertex.class);
    conf = new ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
        NullWritable, IntWritable>(configuration);
    context = mock(Mapper.Context.class);
  }

  @Test
  public void testSimplePartitionStore() {
    PartitionStore<IntWritable, IntWritable, NullWritable, IntWritable>
        partitionStore = new SimplePartitionStore<IntWritable, IntWritable,
                NullWritable, IntWritable>(conf, context);
    testReadWrite(partitionStore, conf);
    partitionStore.shutdown();
  }

  @Test
  public void testUnsafePartitionSerializationClass() throws IOException {
    conf.setPartitionClass(ByteArrayPartition.class);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v1 =
        conf.createVertex();
    v1.initialize(new IntWritable(1), new IntWritable(1));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v2 =
        conf.createVertex();
    v2.initialize(new IntWritable(2), new IntWritable(2));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v3 =
        conf.createVertex();
    v3.initialize(new IntWritable(3), new IntWritable(3));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v4 =
        conf.createVertex();
    v4.initialize(new IntWritable(4), new IntWritable(4));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v5 =
        conf.createVertex();
    v5.initialize(new IntWritable(5), new IntWritable(5));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v6 =
        conf.createVertex();
    v6.initialize(new IntWritable(6), new IntWritable(6));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v7 =
        conf.createVertex();
    v7.initialize(new IntWritable(7), new IntWritable(7));

    Partition<IntWritable, IntWritable, NullWritable,
        IntWritable> partition =
        createPartition(conf, 3, v1, v2, v3, v4, v5, v6, v7);
    assertEquals(3, partition.getId());
    assertEquals(0, partition.getEdgeCount());
    assertEquals(7, partition.getVertexCount());
    UnsafeByteArrayOutputStream outputStream = new
        UnsafeByteArrayOutputStream();
    partition.write(outputStream);
    UnsafeByteArrayInputStream inputStream = new UnsafeByteArrayInputStream(
        outputStream.getByteArray(), 0, outputStream.getPos());
    Partition<IntWritable, IntWritable, NullWritable,
        IntWritable> deserializatedPartition = conf.createPartition(-1,
        context);
    deserializatedPartition.readFields(inputStream);

    assertEquals(3, deserializatedPartition.getId());
    assertEquals(0, deserializatedPartition.getEdgeCount());
    assertEquals(7, deserializatedPartition.getVertexCount());
  }

  @Test
  public void testDiskBackedPartitionStore() throws IOException {
    File directory = Files.createTempDir();
    GiraphConstants.PARTITIONS_DIRECTORY.set(
        conf, new File(directory, "giraph_partitions").toString());
    GiraphConstants.USE_OUT_OF_CORE_GRAPH.set(conf, true);
    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, 1);

    PartitionStore<IntWritable, IntWritable, NullWritable, IntWritable>
        partitionStore = new DiskBackedPartitionStore<IntWritable,
                        IntWritable, NullWritable, IntWritable>(conf, context);
    testReadWrite(partitionStore, conf);
    partitionStore.shutdown();

    GiraphConstants.MAX_PARTITIONS_IN_MEMORY.set(conf, 2);
    partitionStore = new DiskBackedPartitionStore<IntWritable,
            IntWritable, NullWritable, IntWritable>(conf, context);
    testReadWrite(partitionStore, conf);
    partitionStore.shutdown();
    FileUtils.deleteDirectory(directory);
  }

  /**
   * Test reading/writing to/from a partition store
   *
   * @param partitionStore Partition store to test
   * @param conf Configuration to use
   */
  public void testReadWrite(
      PartitionStore<IntWritable, IntWritable,
          NullWritable, IntWritable> partitionStore,
      ImmutableClassesGiraphConfiguration<IntWritable, IntWritable,
          NullWritable, IntWritable> conf) {
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v1 =
        conf.createVertex();
    v1.initialize(new IntWritable(1), new IntWritable(1));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v2 =
        conf.createVertex();
    v2.initialize(new IntWritable(2), new IntWritable(2));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v3 =
        conf.createVertex();
    v3.initialize(new IntWritable(3), new IntWritable(3));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v4 =
        conf.createVertex();
    v4.initialize(new IntWritable(4), new IntWritable(4));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v5 =
        conf.createVertex();
    v5.initialize(new IntWritable(5), new IntWritable(5));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v6 =
        conf.createVertex();
    v6.initialize(new IntWritable(7), new IntWritable(7));
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v7 =
        conf.createVertex();
    v7.initialize(new IntWritable(7), new IntWritable(7));

    partitionStore.addPartition(createPartition(conf, 1, v1, v2));
    partitionStore.addPartition(createPartition(conf, 2, v3));
    partitionStore.addPartition(createPartition(conf, 2, v4));
    partitionStore.addPartition(createPartition(conf, 3, v5));
    partitionStore.addPartition(createPartition(conf, 1, v6));
    partitionStore.addPartition(createPartition(conf, 4, v7));

    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition1 =
        partitionStore.getPartition(1);
    partitionStore.putPartition(partition1);
    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition2 =
        partitionStore.getPartition(2);
    partitionStore.putPartition(partition2);
    Partition<IntWritable, IntWritable, NullWritable,
        IntWritable> partition3 = partitionStore.removePartition(3);
    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition4 =
        partitionStore.getPartition(4);
    partitionStore.putPartition(partition4);

    assertEquals(3, partitionStore.getNumPartitions());
    assertEquals(3, Iterables.size(partitionStore.getPartitionIds()));
    int partitionsNumber = 0;
    for (Integer partitionId : partitionStore.getPartitionIds()) {
      Partition<IntWritable, IntWritable, NullWritable, IntWritable> p = 
          partitionStore.getPartition(partitionId);
      partitionStore.putPartition(p);
      partitionsNumber++;
    }
    assertEquals(3, partitionsNumber);
    assertTrue(partitionStore.hasPartition(1));
    assertTrue(partitionStore.hasPartition(2));
    assertFalse(partitionStore.hasPartition(3));
    assertTrue(partitionStore.hasPartition(4));
    assertEquals(3, partition1.getVertexCount());
    assertEquals(2, partition2.getVertexCount());
    assertEquals(1, partition3.getVertexCount());
    assertEquals(1, partition4.getVertexCount());

    partitionStore.deletePartition(2);

    assertEquals(2, partitionStore.getNumPartitions());
  }
}
