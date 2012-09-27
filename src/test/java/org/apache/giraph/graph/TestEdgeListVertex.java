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

import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.SystemTime;
import org.apache.giraph.utils.Time;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link EdgeListVertex}.
 */
public class TestEdgeListVertex {
  /** Instantiated vertex filled in from setup() */
  private IFDLEdgeListVertex vertex;
  /** Job filled in by setup() */
  private GiraphJob job;
  /** Immutable classes giraph configuration */
  private ImmutableClassesGiraphConfiguration<IntWritable, FloatWritable,
        DoubleWritable, LongWritable> configuration;

  /**
   * Simple instantiable class that extends {@link EdgeListVertex}.
   */
  public static class IFDLEdgeListVertex extends
      EdgeListVertex<IntWritable, FloatWritable, DoubleWritable,
      LongWritable> {
    @Override
    public void compute(Iterable<LongWritable> messages) throws IOException {
    }
  }

  @Before
  public void setUp() {
    try {
      job = new GiraphJob("TestEdgeArrayVertex");
    } catch (IOException e) {
      throw new RuntimeException("setUp: Failed", e);
    }
    job.getConfiguration().setVertexClass(IFDLEdgeListVertex.class);
    Configuration conf = job.getConfiguration();
    conf.setClass(GiraphConfiguration.VERTEX_ID_CLASS, IntWritable.class,
        WritableComparable.class);
    conf.setClass(GiraphConfiguration.VERTEX_VALUE_CLASS, FloatWritable.class,
        Writable.class);
    conf.setClass(GiraphConfiguration.EDGE_VALUE_CLASS, DoubleWritable.class,
        Writable.class);
    conf.setClass(GiraphConfiguration.MESSAGE_VALUE_CLASS, LongWritable.class,
        Writable.class);
    configuration =
        new ImmutableClassesGiraphConfiguration<IntWritable,
            FloatWritable, DoubleWritable, LongWritable>(
            job.getConfiguration());
    vertex = (IFDLEdgeListVertex) configuration.createVertex();
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
    assertEquals(vertex.getNumEdges(), 1000);
    for (Edge<IntWritable, DoubleWritable> edge : vertex.getEdges()) {
      assertEquals(edge.getValue().get(),
          edge.getTargetVertexId().get() * 2.0d, 0d);
    }
    assertEquals(vertex.removeEdge(new IntWritable(500)),
        new DoubleWritable(1000));
    assertEquals(vertex.getNumEdges(), 999);
  }

  @Test
  public void testGetEdges() {
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    for (int i = 1000; i > 0; --i) {
      edgeMap.put(new IntWritable(i), new DoubleWritable(i * 3.0));
    }
    vertex.initialize(null, null, edgeMap, null);
    assertEquals(vertex.getNumEdges(), 1000);
    assertEquals(vertex.getEdgeValue(new IntWritable(600)),
        new DoubleWritable(600 * 3.0));
    assertEquals(vertex.removeEdge(new IntWritable(600)),
        new DoubleWritable(600 * 3.0));
    assertEquals(vertex.getNumEdges(), 999);
    assertEquals(vertex.getEdgeValue(new IntWritable(500)),
        new DoubleWritable(500 * 3.0));
    assertEquals(vertex.getEdgeValue(new IntWritable(700)),
        new DoubleWritable(700 * 3.0));
  }

  @Test
  public void testAddRemoveEdges() {
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    vertex.initialize(null, null, edgeMap, null);
    assertEquals(vertex.getNumEdges(), 0);
    assertTrue(vertex.addEdge(new IntWritable(2),
        new DoubleWritable(2.0)));
    assertEquals(vertex.getNumEdges(), 1);
    assertEquals(vertex.getEdgeValue(new IntWritable(2)),
        new DoubleWritable(2.0));
    assertTrue(vertex.addEdge(new IntWritable(4),
        new DoubleWritable(4.0)));
    assertTrue(vertex.addEdge(new IntWritable(3),
        new DoubleWritable(3.0)));
    assertTrue(vertex.addEdge(new IntWritable(1),
        new DoubleWritable(1.0)));
    assertEquals(vertex.getNumEdges(), 4);
    assertNull(vertex.getEdgeValue(new IntWritable(5)));
    assertNull(vertex.getEdgeValue(new IntWritable(0)));
    for (Edge<IntWritable, DoubleWritable> edge : vertex.getEdges()) {
      assertEquals(edge.getTargetVertexId().get() * 1.0d,
          edge.getValue().get(), 0d);
    }
    assertNotNull(vertex.removeEdge(new IntWritable(1)));
    assertEquals(vertex.getNumEdges(), 3);
    assertNotNull(vertex.removeEdge(new IntWritable(3)));
    assertEquals(vertex.getNumEdges(), 2);
    assertNotNull(vertex.removeEdge(new IntWritable(2)));
    assertEquals(vertex.getNumEdges(), 1);
    assertNotNull(vertex.removeEdge(new IntWritable(4)));
    assertEquals(vertex.getNumEdges(), 0);
  }

  @Test
  public void testGiraphTransferRegulator() {
     job.getConfiguration()
       .setInt(GiraphTransferRegulator.MAX_VERTICES_PER_TRANSFER, 1);
     job.getConfiguration()
       .setInt(GiraphTransferRegulator.MAX_EDGES_PER_TRANSFER, 3);
     Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
     edgeMap.put(new IntWritable(2), new DoubleWritable(22));
     edgeMap.put(new IntWritable(3), new DoubleWritable(33));
     edgeMap.put(new IntWritable(4), new DoubleWritable(44));
     vertex.initialize(null, null, edgeMap, null);
     GiraphTransferRegulator gtr =
       new GiraphTransferRegulator(job.getConfiguration());
     PartitionOwner owner = mock(PartitionOwner.class);
     when(owner.getPartitionId()).thenReturn(57);
     assertFalse(gtr.transferThisPartition(owner));
     gtr.incrementCounters(owner, vertex);
     assertTrue(gtr.transferThisPartition(owner));
  }

  @Test
  public void testSerialize() {
    final int edgesCount = 1000;
    Map<IntWritable, DoubleWritable> edgeMap = Maps.newHashMap();
    for (int i = edgesCount; i > 0; --i) {
      edgeMap.put(new IntWritable(i), new DoubleWritable(i * 2.0));
    }
    List<LongWritable> messageList = Lists.newArrayList();
    messageList.add(new LongWritable(4));
    messageList.add(new LongWritable(5));
    vertex.initialize(
        new IntWritable(2), new FloatWritable(3.0f), edgeMap, messageList);
    long serializeNanosStart = SystemTime.getInstance().getNanoseconds();
    byte[] byteArray = WritableUtils.writeToByteArray(vertex);
    long serializeNanos = SystemTime.getInstance().getNanosecondsSince(
        serializeNanosStart);
    System.out.println("testSerialize: Serializing took " +
        serializeNanos +
        " ns for " + byteArray.length + " bytes " +
        (byteArray.length * 1f * Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec");
    IFDLEdgeListVertex readVertex = (IFDLEdgeListVertex)
      configuration.createVertex();
    long deserializeNanosStart = SystemTime.getInstance().getNanoseconds();
    WritableUtils.readFieldsFromByteArray(byteArray, readVertex);
    long deserializeNanos = SystemTime.getInstance().getNanosecondsSince(
        deserializeNanosStart);
    System.out.println("testSerialize: Deserializing took " +
        deserializeNanos +
        " ns for " + byteArray.length + " bytes " +
        (byteArray.length * 1f * Time.NS_PER_SECOND / deserializeNanos) +
        " bytes / sec");

    assertEquals(vertex.getId(), readVertex.getId());
    assertEquals(vertex.getValue(), readVertex.getValue());
    assertEquals(Lists.newArrayList(vertex.getEdges()),
        Lists.newArrayList(readVertex.getEdges()));
    assertEquals(Lists.newArrayList(vertex.getMessages()),
        Lists.newArrayList(readVertex.getMessages()));
  }
}
