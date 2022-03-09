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
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class IdAndNullArrayEdgesTest {
  private static Edge<LongWritable, NullWritable> createEdge(long id) {
    return EdgeFactory.create(new LongWritable(id));
  }

  private static void assertEdges(IdAndNullArrayEdges edges, long... expected) {
    int index = 0;
    for (Edge<LongWritable, NullWritable> edge :
      (Iterable<Edge<LongWritable, NullWritable>>) edges) {
      Assert.assertEquals(expected[index], edge.getTargetVertexId().get());
      index++;
    }
    Assert.assertEquals(expected.length, index);
  }

  private IdAndNullArrayEdges getEdges() {
    GiraphConfiguration gc = new GiraphConfiguration();
    GiraphConstants.VERTEX_ID_CLASS.set(gc, LongWritable.class);
    GiraphConstants.EDGE_VALUE_CLASS.set(gc, NullWritable.class);
    ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable> conf =
        new ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable>(gc);
    IdAndNullArrayEdges ret = new IdAndNullArrayEdges();
    ret.setConf(
      new ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable>(conf));
    return ret;
  }

  @Test
  public void testEdges() {
    IdAndNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(1), createEdge(2), createEdge(4));

    edges.initialize(initialEdges);
    assertEdges(edges, 1, 2, 4);

    edges.add(EdgeFactory.createReusable(new LongWritable(3)));
    assertEdges(edges, 1, 2, 4, 3); // order matters, it's an array

    edges.remove(new LongWritable(2));
    assertEdges(edges, 1, 3, 4);
  }

  @Test
  public void testInitialize() {
    IdAndNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
      createEdge(1), createEdge(2), createEdge(4));

    edges.initialize(initialEdges);
    assertEdges(edges, 1, 2, 4);

    edges.add(EdgeFactory.createReusable(new LongWritable(3)));
    assertEdges(edges, 1, 2, 4, 3);

    edges.initialize();
    assertEquals(0, edges.size());
  }

  @Test
  public void testMutateEdges() {
    IdAndNullArrayEdges edges = getEdges();

    edges.initialize();

    // Add 10 edges with id i, for i = 0..9
    for (int i = 0; i < 10; ++i) {
      edges.add(createEdge(i));
    }

    // Use the mutable iterator to remove edges with even id
    Iterator<MutableEdge<LongWritable, NullWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      if (edgeIt.next().getTargetVertexId().get() % 2 == 0) {
        edgeIt.remove();
      }
    }

    // We should now have 5 edges
    assertEquals(5, edges.size());
    // The edge ids should be all odd
    for (Edge<LongWritable, NullWritable> edge :
      (Iterable<Edge<LongWritable, NullWritable>>) edges) {
      assertEquals(1, edge.getTargetVertexId().get() % 2);
    }
  }

  @Test
  public void testSerialization() throws IOException {
    IdAndNullArrayEdges edges = getEdges();

    edges.initialize();

    // Add 10 edges with id i, for i = 0..9
    for (int i = 0; i < 10; ++i) {
      edges.add(createEdge(i));
    }

    // Use the mutable iterator to remove edges with even id
    Iterator<MutableEdge<LongWritable, NullWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      if (edgeIt.next().getTargetVertexId().get() % 2 == 0) {
        edgeIt.remove();
      }
    }

    // We should now have 5 edges
    assertEdges(edges, 9, 1, 7, 3, 5); // id order matter because of the implementation

    ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
    DataOutputStream tempBuffer = new DataOutputStream(arrayStream);

    edges.write(tempBuffer);
    tempBuffer.close();

    byte[] binary = arrayStream.toByteArray();

    assertTrue("Serialized version should not be empty ", binary.length > 0);

    edges = getEdges();
    edges.readFields(new DataInputStream(new ByteArrayInputStream(binary)));

    assertEquals(5, edges.size());

    long[] ids = new long[]{9, 1, 7, 3, 5};
    int index = 0;

    for (Edge<LongWritable, NullWritable> edge :
      (Iterable<Edge<LongWritable, NullWritable>>) edges) {
      assertEquals(ids[index], edge.getTargetVertexId().get());
      index++;
    }
    assertEquals(ids.length, index);
  }

  @Test
  public void testParallelEdges() {
    IdAndNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(2), createEdge(2), createEdge(2));

    edges.initialize(initialEdges);
    assertEquals(3, edges.size());

    edges.remove(new LongWritable(2));
    assertEquals(0, edges.size());

    edges.add(EdgeFactory.create(new LongWritable(2)));
    assertEquals(1, edges.size());

    assertEquals(1, edges.size());
  }

  @Test
  public void testEdgeValues() {
    IdAndNullArrayEdges edges = getEdges();
    Set<Long> testValues = new HashSet<Long>();
    testValues.add(0L);
    testValues.add((long) Integer.MAX_VALUE);
    testValues.add(Long.MAX_VALUE);

    List<Edge<LongWritable, NullWritable>> initialEdges =
      new ArrayList<Edge<LongWritable, NullWritable>>();
    for(Long id : testValues) {
      initialEdges.add(createEdge(id));
    }

    edges.initialize(initialEdges);

    Iterator<MutableEdge<LongWritable, NullWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      long value = edgeIt.next().getTargetVertexId().get();
      assertTrue("Unknown edge found " + value, testValues.remove(value));
    }
  }

  @Test
  public void testAddedSmallerValues() {
    IdAndNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(100));

    edges.initialize(initialEdges);

    for (int i=0; i<16; i++) {
      edges.add(createEdge(i));
    }

    assertEquals(17, edges.size());
  }
}
