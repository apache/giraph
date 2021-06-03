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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.giraph.graph.TestVertexAndEdges.instantiateOutEdges;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link OutEdges} implementations with null edge values.
 */
public class TestNullValueEdges {
  /** {@link OutEdges} classes to be tested. */
  private Collection<Class<? extends MutableOutEdges>>
      edgesClasses = Lists.newArrayList();

  @Before
  public void setUp() {
    edgesClasses.add(LongNullArrayEdges.class);
    edgesClasses.add(LongNullHashSetEdges.class);
  }

  @Test
  public void testEdges() {
    for (Class<? extends OutEdges> edgesClass : edgesClasses) {
      testEdgesClass(edgesClass);
    }
  }

  private void testEdgesClass(
      Class<? extends OutEdges> edgesClass) {
    OutEdges<LongWritable, NullWritable> edges =
        (OutEdges<LongWritable, NullWritable>)
            instantiateOutEdges(edgesClass);

    List<Edge<LongWritable, NullWritable>> initialEdges = Lists.newArrayList(
        EdgeFactory.create(new LongWritable(1)),
        EdgeFactory.create(new LongWritable(2)),
        EdgeFactory.create(new LongWritable(3)));

    edges.initialize(initialEdges);
    assertEquals(3, edges.size());

    edges.add(EdgeFactory.createReusable(new LongWritable(4)));
    assertEquals(4, edges.size());

    edges.remove(new LongWritable(2));
    assertEquals(3, edges.size());
  }

  /**
   * Test in-place edge mutations via the iterable returned by {@link
   * org.apache.giraph.graph.Vertex#getMutableEdges()}.
   */
  @Test
  public void testMutateEdges() {
    for (Class<? extends MutableOutEdges> edgesClass : edgesClasses) {
      testMutateEdgesClass(edgesClass);
    }
  }

  private void testMutateEdgesClass(
      Class<? extends MutableOutEdges> edgesClass) {
    MutableOutEdges<LongWritable, NullWritable> edges =
       (MutableOutEdges<LongWritable, NullWritable>)
           instantiateOutEdges(edgesClass);

    edges.initialize();

    // Add 10 edges with id i, for i = 0..9
    for (int i = 0; i < 10; ++i) {
      edges.add(EdgeFactory.create(new LongWritable(i)));
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
}
