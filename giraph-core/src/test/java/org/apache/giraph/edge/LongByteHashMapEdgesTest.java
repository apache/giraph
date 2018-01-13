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
package org.apache.giraph.edge;

import com.google.common.collect.Lists;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class LongByteHashMapEdgesTest {
  private static Edge<LongWritable, ByteWritable> createEdge(long id, byte value) {
    return EdgeFactory.create(new LongWritable(id), new ByteWritable(value));
  }

  private static void assertEdges(LongByteHashMapEdges edges, long[] expectedIds,
                                  byte[] expectedValues) {
    Assert.assertEquals(expectedIds.length, edges.size());
    for (int i = 0; i< expectedIds.length; i++) {
       ByteWritable value = edges.getEdgeValue(new LongWritable(expectedIds[i]));
       assertNotNull(value);
      assertEquals(expectedValues[i], value.get());
    }
  }

  @Test
  public void testEdges() {
    LongByteHashMapEdges edges = new LongByteHashMapEdges();

    List<Edge<LongWritable, ByteWritable>> initialEdges = Lists.newArrayList(
      createEdge(1, (byte) 99), createEdge(2, (byte) 77), createEdge(4, (byte) 66));

    edges.initialize(initialEdges);
    assertEdges(edges, new long[]{1, 2, 4}, new byte[]{99, 77, 66});

    edges.add(EdgeFactory.createReusable(new LongWritable(3), new ByteWritable((byte) 55)));
    assertEdges(edges, new long[]{1, 2, 3, 4}, new byte[]{99, 77, 55, 66});

    edges.remove(new LongWritable(2));
    assertEdges(edges, new long[]{1, 3, 4}, new byte[]{99, 55, 66});
  }

  @Test
  public void testMutateEdges() {
    LongByteHashMapEdges edges = new LongByteHashMapEdges();

    edges.initialize();

    // Add 10 edges with id and value set to i, for i = 0..9
    for (int i = 0; i < 10; ++i) {
      edges.add(createEdge(i, (byte) i));
    }

    // Use the mutable iterator to remove edges with even id
    Iterator<MutableEdge<LongWritable, ByteWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      if (edgeIt.next().getTargetVertexId().get() % 2 == 0) {
        edgeIt.remove();
      }
    }

    // We should now have 5 edges
    assertEquals(5, edges.size());
    // The edge ids should be all odd
    for (Edge<LongWritable, ByteWritable> edge : edges) {
      assertEquals(1, edge.getTargetVertexId().get() % 2);
      assertEquals(1, edge.getValue().get() % 2);
    }
  }

  @Test
  public void testSerialization() throws IOException {
    LongByteHashMapEdges edges = new LongByteHashMapEdges();

    edges.initialize();

    // Add 10 edges with id and value set to i, for i = 0..9
    for (int i = 0; i < 10; ++i) {
      edges.add(createEdge(i, (byte) i));
    }

    edges.trim();

    // Use the mutable iterator to remove edges with even id
    Iterator<MutableEdge<LongWritable, ByteWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      if (edgeIt.next().getTargetVertexId().get() % 2 == 0) {
        edgeIt.remove();
      }
    }

    // We should now have 5 edges
    assertEdges(edges, new long[]{1, 3, 5, 7, 9}, new byte[]{1, 3, 5, 7, 9});

    ExtendedDataOutput tempBuffer = new UnsafeByteArrayOutputStream();

    edges.write(tempBuffer);

    DataInput input = new UnsafeByteArrayInputStream(
      tempBuffer.getByteArray(), 0, tempBuffer.getPos());

    edges = new LongByteHashMapEdges();
    edges.readFields(input);

    assertEquals(5, edges.size());

    for (Edge<LongWritable, ByteWritable> edge : edges) {
      assertEquals(1, edge.getTargetVertexId().get() % 2);
      assertEquals(1, edge.getValue().get() % 2);
    }
  }

  /**
   * This implementation does not allow parallel edges.
   */
  @Test
  public void testParallelEdges() {
    LongByteHashMapEdges edges = new LongByteHashMapEdges();

    List<Edge<LongWritable, ByteWritable>> initialEdges = Lists.newArrayList(
      createEdge(2, (byte) 1), createEdge(2, (byte) 2), createEdge(2, (byte) 3));

    edges.initialize(initialEdges);
    assertEquals(1, edges.size());

    edges.remove(new LongWritable(2));
    assertEquals(0, edges.size());

    edges.add(EdgeFactory.create(new LongWritable(2), new ByteWritable((byte) 4)));
    assertEquals(1, edges.size());

    edges.trim();
    assertEquals(1, edges.size());
  }
}
