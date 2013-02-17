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

package org.apache.giraph.vertex;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.EdgeFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Test all multigraph mutable vertices.
 */
public class TestMultiGraphVertex {
  private Collection<Class<? extends Vertex<IntWritable, IntWritable,
        IntWritable, IntWritable>>> vertexClasses = Lists.newArrayList();

  public static class MyMultiGraphEdgeListVertex
      extends MultiGraphEdgeListVertex<IntWritable, IntWritable, IntWritable,
            IntWritable> {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException { }
  }

  public static class MyMultiGraphByteArrayVertex
      extends MultiGraphByteArrayVertex<IntWritable, IntWritable,
                  IntWritable, IntWritable> {
    @Override
    public void compute(Iterable<IntWritable> messages) throws IOException { }
  }

  @Before
  public void setUp() {
    vertexClasses.add(MyMultiGraphEdgeListVertex.class);
    vertexClasses.add(MyMultiGraphByteArrayVertex.class);
  }

  @Test
  public void testAddRemoveEdges() {
    for (Class<? extends Vertex<IntWritable, IntWritable,
        IntWritable, IntWritable>> vertexClass : vertexClasses) {
      testAddRemoveEdgesVertexClass(vertexClass);
    }
  }

  private void testAddRemoveEdgesVertexClass(Class<? extends Vertex<IntWritable,
      IntWritable, IntWritable, IntWritable>> vertexClass) {
    MutableVertex<IntWritable, IntWritable, IntWritable,
            IntWritable> vertex = instantiateVertex(vertexClass);

    assertEquals(vertex.removeEdges(new IntWritable(1)), 0);

    // We test a few different patterns for duplicate edges,
    // in order to catch corner cases:

    // Edge list of form: [A, B, A]
    vertex.addEdge(EdgeFactory.create(new IntWritable(1), new IntWritable(1)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(2), new IntWritable(2)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(1), new IntWritable(10)));
    assertEquals(vertex.getNumEdges(), 3);
    assertEquals(vertex.removeEdges(new IntWritable(1)), 2);
    assertEquals(vertex.getNumEdges(), 1);

    // Edge list of form: [A, B, B]
    vertex = instantiateVertex(vertexClass);
    vertex.addEdge(EdgeFactory.create(new IntWritable(2), new IntWritable(2)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(1), new IntWritable(1)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(1), new IntWritable(10)));
    assertEquals(vertex.getNumEdges(), 3);
    assertEquals(vertex.removeEdges(new IntWritable(1)), 2);
    assertEquals(vertex.getNumEdges(), 1);

    // Edge list of form: [A, A, B]
    vertex = instantiateVertex(vertexClass);
    vertex.addEdge(EdgeFactory.create(new IntWritable(1), new IntWritable(1)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(1), new IntWritable(10)));
    vertex.addEdge(EdgeFactory.create(new IntWritable(2), new IntWritable(2)));
    assertEquals(vertex.getNumEdges(), 3);
    assertEquals(vertex.removeEdges(new IntWritable(1)), 2);
    assertEquals(vertex.getNumEdges(), 1);
  }

  private MutableVertex<IntWritable, IntWritable, IntWritable, IntWritable>
  instantiateVertex(Class<? extends Vertex<IntWritable, IntWritable,
      IntWritable, IntWritable>> vertexClass) {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    giraphConfiguration.setVertexClass(vertexClass);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration =
        new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    MutableVertex<IntWritable, IntWritable, IntWritable,
        IntWritable> vertex =
        (MutableVertex<IntWritable, IntWritable,
            IntWritable, IntWritable>)
            immutableClassesGiraphConfiguration.createVertex();
    vertex.initialize(new IntWritable(), new IntWritable());
    return vertex;
  }
}
