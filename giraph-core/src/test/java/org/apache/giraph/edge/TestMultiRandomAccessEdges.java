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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.apache.giraph.graph.TestVertexAndEdges.instantiateOutEdges;
import static org.junit.Assert.assertEquals;

/**
 * Tests {@link MultiRandomAccessOutEdges} implementations.
 */
public class TestMultiRandomAccessEdges {
  /** {@link MultiRandomAccessOutEdges} classes to be tested. */
  private Collection<Class<? extends MultiRandomAccessOutEdges>>
      edgesClasses = Lists.newArrayList();

  @Before
  public void setUp() {
    edgesClasses.add(HashMultimapEdges.class);
  }

  /**
   * Ensures that all {@link MultiRandomAccessOutEdges} implementations
   * correctly return edge values.
   */
  @Test
  public void testParallelEdges() {
    for (Class<? extends MultiRandomAccessOutEdges> edgesClass :
        edgesClasses) {
      testParallelEdgesClass(edgesClass);
    }
  }

  private void testParallelEdgesClass(
      Class<? extends MultiRandomAccessOutEdges> edgesClass) {
    MultiRandomAccessOutEdges<LongWritable, DoubleWritable> edges =
        (MultiRandomAccessOutEdges<LongWritable, DoubleWritable>)
            instantiateOutEdges(edgesClass);

    // Initial edges list contains parallel edges.
    List<Edge<LongWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
        EdgeFactory.create(new LongWritable(1), new DoubleWritable(1)),
        EdgeFactory.create(new LongWritable(2), new DoubleWritable(2)),
        EdgeFactory.create(new LongWritable(3), new DoubleWritable(3)),
        EdgeFactory.create(new LongWritable(2), new DoubleWritable(20)));

    edges.initialize(initialEdges);

    assertEquals(2,
        Iterables.size(edges.getAllEdgeValues(new LongWritable(2))));
    assertEquals(1,
        Iterables.size(edges.getAllEdgeValues(new LongWritable(1))));
    assertEquals(0,
        Iterables.size(edges.getAllEdgeValues(new LongWritable(42))));
  }
}
