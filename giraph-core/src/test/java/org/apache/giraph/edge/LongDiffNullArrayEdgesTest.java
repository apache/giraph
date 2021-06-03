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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;


public class LongDiffNullArrayEdgesTest {
  private static Edge<LongWritable, NullWritable> createEdge(long id) {
    return EdgeFactory.create(new LongWritable(id));
  }

  private static void assertEdges(LongDiffNullArrayEdges edges, long... expected) {
    int index = 0;
    for (Edge<LongWritable, NullWritable> edge : edges) {
      Assert.assertEquals(expected[index], edge.getTargetVertexId().get());
      index++;
    }
    Assert.assertEquals(expected.length, index);
  }

  @Test
  public void testEdges() {
    LongDiffNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(1), createEdge(2), createEdge(4));

    edges.initialize(initialEdges);
    assertEdges(edges, 1, 2, 4);

    edges.add(EdgeFactory.createReusable(new LongWritable(3)));
    assertEdges(edges, 1, 2, 3, 4);

    edges.remove(new LongWritable(2));
    assertEdges(edges, 1, 3, 4);
  }

  @Test
  public void testPositiveAndNegativeEdges() {
    LongDiffNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(1), createEdge(-2), createEdge(3), createEdge(-4));

    edges.initialize(initialEdges);
    assertEdges(edges, -4, -2, 1, 3);

    edges.add(EdgeFactory.createReusable(new LongWritable(5)));
    assertEdges(edges, -4, -2, 1, 3, 5);

    edges.remove(new LongWritable(-2));
    assertEdges(edges, -4, 1, 3, 5);
  }

  @Test
  public void testMutateEdges() {
    LongDiffNullArrayEdges edges = getEdges();

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
    for (Edge<LongWritable, NullWritable> edge : edges) {
      assertEquals(1, edge.getTargetVertexId().get() % 2);
    }
  }

  @Test
  public void testSerialization() throws IOException {
    LongDiffNullArrayEdges edges = getEdges();

    edges.initialize();

    // Add 10 edges with id i, for i = 0..9
    for (int i = 0; i < 10; ++i) {
      edges.add(createEdge(i));
    }

    edges.trim();

    // Use the mutable iterator to remove edges with even id
    Iterator<MutableEdge<LongWritable, NullWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      if (edgeIt.next().getTargetVertexId().get() % 2 == 0) {
        edgeIt.remove();
      }
    }

    // We should now have 5 edges
    assertEdges(edges, 1, 3, 5, 7, 9);

    ByteArrayOutputStream arrayStream = new ByteArrayOutputStream();
    DataOutputStream tempBuffer = new DataOutputStream(arrayStream);

    edges.write(tempBuffer);

    byte[] binary = arrayStream.toByteArray();

    assertTrue("Serialized version should not be empty ", binary.length > 0);

    edges = getEdges();
    edges.readFields(new UnsafeByteArrayInputStream(binary));

    assertEquals(5, edges.size());

    for (Edge<LongWritable, NullWritable> edge : edges) {
      assertEquals(1, edge.getTargetVertexId().get() % 2);
    }
  }

  @Test
  public void testParallelEdges() {
    LongDiffNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(2), createEdge(2), createEdge(2));

    edges.initialize(initialEdges);
    assertEquals(3, edges.size());

    edges.remove(new LongWritable(2));
    assertEquals(0, edges.size());

    edges.add(EdgeFactory.create(new LongWritable(2)));
    assertEquals(1, edges.size());

    edges.trim();
    assertEquals(1, edges.size());
  }

  @Test
  public void testEdgeValues() {
    LongDiffNullArrayEdges edges = getEdges();
    Set<Long> testValues = new HashSet<Long>();
    testValues.add(0L);
    testValues.add((long) Integer.MAX_VALUE);
    testValues.add(Long.MAX_VALUE);

    // shouldn't be working with negative IDs
    // testValues.add((long) Integer.MIN_VALUE);
    // testValues.add(Long.MIN_VALUE);

    List<Edge<LongWritable, NullWritable>> initialEdges =
        new ArrayList<Edge<LongWritable, NullWritable>>();
    for(Long id : testValues) {
      initialEdges.add(createEdge(id));
    }

    edges.initialize(initialEdges);
    edges.trim();

    Iterator<MutableEdge<LongWritable, NullWritable>> edgeIt =
        edges.mutableIterator();
    while (edgeIt.hasNext()) {
      long value = edgeIt.next().getTargetVertexId().get();
      assertTrue("Unknown edge found " + value, testValues.remove(value));
    }
  }

  private LongDiffNullArrayEdges getEdges() {
    GiraphConfiguration gc = new GiraphConfiguration();
    ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable> conf =
        new ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable>(gc);
    LongDiffNullArrayEdges ret = new LongDiffNullArrayEdges();
    ret.setConf(new ImmutableClassesGiraphConfiguration<LongWritable, Writable, NullWritable>(conf));
    return ret;
  }

  @Test
  public void testAddedSmalerValues() {
    LongDiffNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(100));

    edges.initialize(initialEdges);

    edges.trim();

    for (int i=0; i<16; i++) {
      edges.add(createEdge(i));
    }

    edges.trim();

    assertEquals(17, edges.size());
  }

  @Test(expected=IllegalStateException.class)
  public void testFailSafeOnPotentialOverflow() {
    LongDiffNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(5223372036854775807L), createEdge(-4223372036854775807L));
    edges.initialize(initialEdges);
  }

  @Test
  public void testAvoidOverflowWithZero() {
    LongDiffNullArrayEdges edges = getEdges();

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        createEdge(5223372036854775807L), createEdge(-4223372036854775807L), createEdge(0));
    edges.initialize(initialEdges);
    assertEdges(edges, -4223372036854775807L, 0, 5223372036854775807L);
  }
}
