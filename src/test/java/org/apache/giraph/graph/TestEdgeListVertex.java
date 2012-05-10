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
package org.apache.giraph.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests {@link EdgeListVertex}.
 */
public class TestEdgeListVertex {
  /** Instantiated vertex filled in from setup() */
  private IFDLEdgeListVertex vertex;
  /** Job filled in by setup() */
  private GiraphJob job;

  /**
   * Simple instantiable class that extends {@link EdgeListVertex}.
   */
  private static class IFDLEdgeListVertex extends
      EdgeListVertex<IntWritable, FloatWritable, DoubleWritable,
      LongWritable> {
    @Override
    public void compute(Iterator<LongWritable> msgIterator)
      throws IOException {
    }
  }

  @Before
  public void setUp() {
    try {
      job = new GiraphJob("TestEdgeArrayVertex");
    } catch (IOException e) {
      throw new RuntimeException("setUp: Failed", e);
    }
    job.setVertexClass(IFDLEdgeListVertex.class);
    Configuration conf = job.getConfiguration();
    conf.setClass(GiraphJob.VERTEX_INDEX_CLASS, IntWritable.class,
        WritableComparable.class);
    conf.setClass(GiraphJob.VERTEX_VALUE_CLASS, FloatWritable.class,
        Writable.class);
    conf.setClass(GiraphJob.EDGE_VALUE_CLASS, DoubleWritable.class,
        Writable.class);
    conf.setClass(GiraphJob.MESSAGE_VALUE_CLASS, LongWritable.class,
        Writable.class);
    vertex = (IFDLEdgeListVertex)
      BspUtils.<IntWritable, FloatWritable, DoubleWritable, LongWritable>
      createVertex(conf);
  }

  @Test
  public void testInstantiate() throws IOException {
    assertNotNull(vertex);
  }

  @Test
  public void testEdges() {
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    for (int i = 1000; i > 0; --i) {
      edgeMap.put(new IntWritable(i), new DoubleWritable(i * 2.0));
    }
    vertex.initialize(null, null, edgeMap, null);
    assertEquals(vertex.getNumOutEdges(), 1000);
    int expectedIndex = 1;
    for (IntWritable index : vertex) {
      assertEquals(index.get(), expectedIndex);
      assertEquals(vertex.getEdgeValue(index).get(),
          expectedIndex * 2.0d);
      ++expectedIndex;
    }
    assertEquals(vertex.removeEdge(new IntWritable(500)),
        new DoubleWritable(1000));
    assertEquals(vertex.getNumOutEdges(), 999);
  }

  @Test
  public void testGetEdges() {
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    for (int i = 1000; i > 0; --i) {
      edgeMap.put(new IntWritable(i), new DoubleWritable(i * 3.0));
    }
    vertex.initialize(null, null, edgeMap, null);
    assertEquals(vertex.getNumOutEdges(), 1000);
    assertEquals(vertex.getEdgeValue(new IntWritable(600)),
        new DoubleWritable(600 * 3.0));
    assertEquals(vertex.removeEdge(new IntWritable(600)),
        new DoubleWritable(600 * 3.0));
    assertEquals(vertex.getNumOutEdges(), 999);
    assertEquals(vertex.getEdgeValue(new IntWritable(500)),
        new DoubleWritable(500 * 3.0));
    assertEquals(vertex.getEdgeValue(new IntWritable(700)),
        new DoubleWritable(700 * 3.0));
  }

  @Test
  public void testAddRemoveEdges() {
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    vertex.initialize(null, null, edgeMap, null);
    assertEquals(vertex.getNumOutEdges(), 0);
    assertTrue(vertex.addEdge(new IntWritable(2),
        new DoubleWritable(2.0)));
    assertEquals(vertex.getNumOutEdges(), 1);
    assertEquals(vertex.getEdgeValue(new IntWritable(2)),
        new DoubleWritable(2.0));
    assertTrue(vertex.addEdge(new IntWritable(4),
        new DoubleWritable(4.0)));
    assertTrue(vertex.addEdge(new IntWritable(3),
        new DoubleWritable(3.0)));
    assertTrue(vertex.addEdge(new IntWritable(1),
        new DoubleWritable(1.0)));
    assertEquals(vertex.getNumOutEdges(), 4);
    assertNull(vertex.getEdgeValue(new IntWritable(5)));
    assertNull(vertex.getEdgeValue(new IntWritable(0)));
    int i = 1;
    for (IntWritable edgeDestId : vertex) {
      assertEquals(i, edgeDestId.get());
      assertEquals(i * 1.0d, vertex.getEdgeValue(edgeDestId).get());
      ++i;
    }
    assertNotNull(vertex.removeEdge(new IntWritable(1)));
    assertEquals(vertex.getNumOutEdges(), 3);
    assertNotNull(vertex.removeEdge(new IntWritable(3)));
    assertEquals(vertex.getNumOutEdges(), 2);
    assertNotNull(vertex.removeEdge(new IntWritable(2)));
    assertEquals(vertex.getNumOutEdges(), 1);
    assertNotNull(vertex.removeEdge(new IntWritable(4)));
    assertEquals(vertex.getNumOutEdges(), 0);
  }

  @Test
  public void testSerialize() {
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    for (int i = 1000; i > 0; --i) {
      edgeMap.put(new IntWritable(i), new DoubleWritable(i * 2.0));
    }
    List<LongWritable> messageList = Lists.newArrayList();
    messageList.add(new LongWritable(4));
    messageList.add(new LongWritable(5));
    vertex.initialize(
        new IntWritable(2), new FloatWritable(3.0f), edgeMap, messageList);
    byte[] byteArray = WritableUtils.writeToByteArray(vertex);
    IFDLEdgeListVertex readVertex = (IFDLEdgeListVertex)
      BspUtils.<IntWritable, FloatWritable, DoubleWritable, LongWritable>
      createVertex(job.getConfiguration());
    WritableUtils.readFieldsFromByteArray(byteArray, readVertex);
    assertEquals(vertex, readVertex);
  }
}
