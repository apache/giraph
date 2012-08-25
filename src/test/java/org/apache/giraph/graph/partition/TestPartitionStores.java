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

package org.apache.giraph.graph.partition;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.IntIntNullIntVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;

/**
 * Test case for partition stores.
 */
public class TestPartitionStores {
  private static class MyVertex extends IntIntNullIntVertex {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException {}
  }

  private Partition<IntWritable, IntWritable, NullWritable,
      IntWritable> createPartition(Configuration conf, Integer id,
                                   Vertex<IntWritable, IntWritable,
                                       NullWritable, IntWritable>... vertices) {
    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition =
        new Partition<IntWritable, IntWritable, NullWritable,
            IntWritable>(conf, id);
    for (Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v :
        vertices) {
      partition.putVertex(v);
    }
    return partition;
  }

  @Test
  public void testSimplePartitionStore() {
    Configuration conf = new Configuration();
    conf.setClass(GiraphJob.VERTEX_CLASS, MyVertex.class,
        Vertex.class);
    conf.setClass(GiraphJob.VERTEX_ID_CLASS, IntWritable.class,
        WritableComparable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS, IntWritable.class,
        Writable.class);
    conf.setClass(GiraphJob.EDGE_VALUE_CLASS, NullWritable.class,
        Writable.class);
    conf.setClass(GiraphJob.MESSAGE_VALUE_CLASS, IntWritable.class,
        Writable.class);

    PartitionStore<IntWritable, IntWritable, NullWritable, IntWritable>
        partitionStore = new SimplePartitionStore<IntWritable, IntWritable,
        NullWritable, IntWritable>(conf);
    testReadWrite(partitionStore, conf);
  }

  @Test
  public void testDiskBackedPartitionStore() {
    Configuration conf = new Configuration();
    conf.setClass(GiraphJob.VERTEX_CLASS, MyVertex.class,
        Vertex.class);
    conf.setClass(GiraphJob.VERTEX_ID_CLASS, IntWritable.class,
        WritableComparable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS, IntWritable.class,
        Writable.class);
    conf.setClass(GiraphJob.EDGE_VALUE_CLASS, NullWritable.class,
        Writable.class);
    conf.setClass(GiraphJob.MESSAGE_VALUE_CLASS, IntWritable.class,
        Writable.class);

    conf.setBoolean(GiraphJob.USE_OUT_OF_CORE_GRAPH, true);
    conf.setInt(GiraphJob.MAX_PARTITIONS_IN_MEMORY, 1);

    PartitionStore<IntWritable, IntWritable, NullWritable, IntWritable>
        partitionStore = new DiskBackedPartitionStore<IntWritable,
                IntWritable, NullWritable, IntWritable>(conf);
    testReadWrite(partitionStore, conf);

    conf.setInt(GiraphJob.MAX_PARTITIONS_IN_MEMORY, 2);
    partitionStore = new DiskBackedPartitionStore<IntWritable,
            IntWritable, NullWritable, IntWritable>(conf);
    testReadWrite(partitionStore, conf);
  }

  public void testReadWrite(PartitionStore<IntWritable, IntWritable,
      NullWritable, IntWritable> partitionStore, Configuration conf) {
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v1 =
        new MyVertex();
    v1.initialize(new IntWritable(1), new IntWritable(1), null, null);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v2 =
        new MyVertex();
    v2.initialize(new IntWritable(2), new IntWritable(2), null, null);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v3 =
        new MyVertex();
    v3.initialize(new IntWritable(3), new IntWritable(3), null, null);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v4 =
        new MyVertex();
    v4.initialize(new IntWritable(4), new IntWritable(4), null, null);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v5 =
        new MyVertex();
    v5.initialize(new IntWritable(5), new IntWritable(5), null, null);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v6 =
        new MyVertex();
    v6.initialize(new IntWritable(7), new IntWritable(7), null, null);
    Vertex<IntWritable, IntWritable, NullWritable, IntWritable> v7 =
        new MyVertex();
    v7.initialize(new IntWritable(7), new IntWritable(7), null, null);

    partitionStore.addPartition(createPartition(conf, 1, v1, v2));
    partitionStore.addPartition(createPartition(conf, 2, v3));
    partitionStore.addPartitionVertices(2, Lists.newArrayList(v4));
    partitionStore.addPartition(createPartition(conf, 3, v5));
    partitionStore.addPartitionVertices(1, Lists.newArrayList(v6));
    partitionStore.addPartitionVertices(4, Lists.newArrayList(v7));

    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition1 =
        partitionStore.getPartition(1);
    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition2 =
        partitionStore.getPartition(2);
    Partition<IntWritable, IntWritable, NullWritable,
        IntWritable> partition3 = partitionStore.removePartition(3);
    Partition<IntWritable, IntWritable, NullWritable, IntWritable> partition4 =
        partitionStore.getPartition(4);

    assertEquals(3, partitionStore.getNumPartitions());
    assertEquals(3, Iterables.size(partitionStore.getPartitionIds()));
    assertEquals(3, Iterables.size(partitionStore.getPartitions()));
    assertTrue(partitionStore.hasPartition(1));
    assertTrue(partitionStore.hasPartition(2));
    assertFalse(partitionStore.hasPartition(3));
    assertTrue(partitionStore.hasPartition(4));
    assertEquals(3, partition1.getVertices().size());
    assertEquals(2, partition2.getVertices().size());
    assertEquals(1, partition3.getVertices().size());
    assertEquals(1, partition4.getVertices().size());

    partitionStore.deletePartition(2);

    assertEquals(2, partitionStore.getNumPartitions());
  }
}
