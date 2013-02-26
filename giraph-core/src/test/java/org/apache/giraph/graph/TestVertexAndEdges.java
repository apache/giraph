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

import com.google.common.collect.Lists;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.edge.HashMultimapEdges;
import org.apache.giraph.edge.LongDoubleArrayEdges;
import org.apache.giraph.edge.LongDoubleHashMapEdges;
import org.apache.giraph.edge.VertexEdges;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.utils.DynamicChannelBufferInputStream;
import org.apache.giraph.utils.DynamicChannelBufferOutputStream;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link Vertex} functionality across the provided {@link org.apache.giraph.edge.VertexEdges}
 * classes.
 */
public class TestVertexAndEdges {
  /** Number of repetitions. */
  public static final int REPS = 100;
  /** {@link org.apache.giraph.edge.VertexEdges} classes to be tested. */
  private Collection<Class<? extends VertexEdges>> edgesClasses =
      Lists.newArrayList();

  /**
   * Dummy concrete vertex.
   */
  public static class TestVertex extends Vertex<LongWritable, FloatWritable,
        DoubleWritable, LongWritable> {
    @Override
    public void compute(Iterable<LongWritable> messages) { }
  }

  @Before
  public void setUp() {
    edgesClasses.add(ByteArrayEdges.class);
    edgesClasses.add(ArrayListEdges.class);
    edgesClasses.add(HashMapEdges.class);
    edgesClasses.add(HashMultimapEdges.class);
    edgesClasses.add(LongDoubleArrayEdges.class);
    edgesClasses.add(LongDoubleHashMapEdges.class);
  }

  private Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable>
  instantiateVertex(Class<? extends VertexEdges> edgesClass) {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    giraphConfiguration.setVertexClass(TestVertex.class);
    giraphConfiguration.setVertexEdgesClass(edgesClass);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration =
        new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    return immutableClassesGiraphConfiguration.createVertex();
  }

  /**
   * Test vertex instantiation, initialization, and updating the vertex value.
   */
  @Test
  public void testVertexIdAndValue() {
    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable> vertex =
        instantiateVertex(ArrayListEdges.class);
    assertNotNull(vertex);
    vertex.initialize(new LongWritable(7), new FloatWritable(3.0f));
    assertEquals(7, vertex.getId().get());
    assertEquals(3.0f, vertex.getValue().get(), 0d);
    vertex.setValue(new FloatWritable(5.5f));
    assertEquals(5.5f, vertex.getValue().get(), 0d);
  }

  public static VertexEdges
  instantiateVertexEdges(Class<? extends VertexEdges> edgesClass) {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    // Needed to extract type arguments in ReflectionUtils.
    giraphConfiguration.setVertexClass(TestVertex.class);
    giraphConfiguration.setVertexEdgesClass(edgesClass);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration =
        new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    return immutableClassesGiraphConfiguration.createVertexEdges();
  }

  /**
   * Test the provided {@link VertexEdges} implementations for instantiation,
   * initialization, edge addition, and edge removal.
   */
  @Test
  public void testEdges() {
    for (Class<? extends VertexEdges> edgesClass : edgesClasses) {
      testEdgesClass(edgesClass);
    }
  }

  private void testEdgesClass(Class<? extends VertexEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable> vertex =
        instantiateVertex(edgesClass);
    VertexEdges<LongWritable, DoubleWritable> vertexEdges =
        instantiateVertexEdges(edgesClass);
    assertNotNull(vertexEdges);

    List<Edge<LongWritable, DoubleWritable>> edges = Lists.newLinkedList();
    for (int i = 1000; i > 0; --i) {
      edges.add(EdgeFactory.create(new LongWritable(i),
          new DoubleWritable(i * 2.0)));
    }

    vertexEdges.initialize(edges);
    vertex.initialize(new LongWritable(1), new FloatWritable(1), vertexEdges);

    assertEquals(20.0, vertex.getEdgeValue(new LongWritable(10)).get(), 0.0);

    assertEquals(1000, vertex.getNumEdges());
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      assertEquals(edge.getTargetVertexId().get() * 2.0d,
          edge.getValue().get(), 0d);
    }
    vertex.removeEdges(new LongWritable(500));
    assertEquals(999, vertex.getNumEdges());
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      assert(edge.getTargetVertexId().get() != 500);
    }
  }

  /**
   * Test {@link Vertex} and {@link VertexEdges} serialization.
   * @throws IOException
   */
  @Test
  public void testSerialize() throws IOException {
    for (Class<? extends VertexEdges> edgesClass : edgesClasses) {
      testSerializeVertexEdgesClass(edgesClass);
      testDynamicChannelBufferSerializeVertexEdgesClass(edgesClass);
      testUnsafeSerializeVertexEdgesClass(edgesClass);
    }
  }

  private Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable>
  buildVertex(Class<? extends VertexEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable> vertex =
        instantiateVertex(edgesClass);
    VertexEdges<LongWritable, DoubleWritable> vertexEdges =
        instantiateVertexEdges(edgesClass);

    int edgesCount = 200;
    List<Edge<LongWritable, DoubleWritable>> edges =
        Lists.newArrayListWithCapacity(edgesCount);
    for (int i = edgesCount; i > 0; --i) {
      edges.add(EdgeFactory.create(new LongWritable(i),
          new DoubleWritable(i * 2.0)));
    }

    vertexEdges.initialize(edges);
    vertex.initialize(new LongWritable(2), new FloatWritable(3.0f),
        vertexEdges);
    return vertex;
  }

  private void testSerializeVertexEdgesClass(
      Class<? extends VertexEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable> vertex =
        buildVertex(edgesClass);

    long serializeNanosStart;
    long serializeNanos = 0;
    byte[] byteArray = null;
    for (int i = 0; i < REPS; ++i) {
      serializeNanosStart = SystemTime.get().getNanoseconds();
      byteArray = WritableUtils.writeToByteArray(vertex);
      serializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          serializeNanosStart);
    }
    serializeNanos /= REPS;
    System.out.println("testSerialize: Serializing took " +
        serializeNanos + " ns for " + byteArray.length + " bytes " +
        (byteArray.length * 1f * Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable>
        readVertex = instantiateVertex(edgesClass);

    long deserializeNanosStart;
    long deserializeNanos = 0;
    for (int i = 0; i < REPS; ++i) {
      deserializeNanosStart = SystemTime.get().getNanoseconds();
      WritableUtils.readFieldsFromByteArray(byteArray, readVertex);
      deserializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          deserializeNanosStart);
    }
    deserializeNanos /= REPS;
    System.out.println("testSerialize: Deserializing took " +
        deserializeNanos + " ns for " + byteArray.length + " bytes " +
        (byteArray.length * 1f * Time.NS_PER_SECOND / deserializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    assertEquals(vertex.getId(), readVertex.getId());
    assertEquals(vertex.getValue(), readVertex.getValue());
    assertTrue(EdgeIterables.sameEdges(vertex.getEdges(), readVertex.getEdges()));
  }

  private void testDynamicChannelBufferSerializeVertexEdgesClass(
      Class<? extends VertexEdges> edgesClass)
      throws IOException {
    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable> vertex =
        buildVertex(edgesClass);

    long serializeNanosStart;
    long serializeNanos = 0;
    DynamicChannelBufferOutputStream outputStream = null;
    for (int i = 0; i <
        REPS; ++i) {
      serializeNanosStart = SystemTime.get().getNanoseconds();
      outputStream =
          new DynamicChannelBufferOutputStream(32);
      vertex.write(outputStream);
      serializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          serializeNanosStart);
    }
    serializeNanos /= REPS;
    System.out.println("testDynamicChannelBufferSerializeVertexEdgesClass: " +
        "Serializing took " + serializeNanos + " ns for " +
        outputStream.getDynamicChannelBuffer().writerIndex() + " bytes " +
        (outputStream.getDynamicChannelBuffer().writerIndex() * 1f *
            Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable>
        readVertex = instantiateVertex(edgesClass);

    long deserializeNanosStart;
    long deserializeNanos = 0;
    for (int i = 0; i < REPS; ++i) {
      deserializeNanosStart = SystemTime.get().getNanoseconds();
      DynamicChannelBufferInputStream inputStream = new
          DynamicChannelBufferInputStream(
          outputStream.getDynamicChannelBuffer());
      readVertex.readFields(inputStream);
      deserializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          deserializeNanosStart);
      outputStream.getDynamicChannelBuffer().readerIndex(0);
    }
    deserializeNanos /= REPS;
    System.out.println("testDynamicChannelBufferSerializeVertexEdgesClass: " +
        "Deserializing took " + deserializeNanos + " ns for " +
        outputStream.getDynamicChannelBuffer().writerIndex() + " bytes " +
        (outputStream.getDynamicChannelBuffer().writerIndex() * 1f *
            Time.NS_PER_SECOND / deserializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    assertEquals(vertex.getId(), readVertex.getId());
    assertEquals(vertex.getValue(), readVertex.getValue());
    assertTrue(EdgeIterables.sameEdges(vertex.getEdges(), readVertex.getEdges()));
  }

  private void testUnsafeSerializeVertexEdgesClass(
      Class<? extends VertexEdges> edgesClass)
      throws IOException {
    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable> vertex =
        buildVertex(edgesClass);

    long serializeNanosStart;
    long serializeNanos = 0;
    UnsafeByteArrayOutputStream outputStream = null;
    for (int i = 0; i <
        REPS; ++i) {
      serializeNanosStart = SystemTime.get().getNanoseconds();
      outputStream =
          new UnsafeByteArrayOutputStream(32);
      vertex.write(outputStream);
      serializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          serializeNanosStart);
    }
    serializeNanos /= REPS;
    System.out.println("testUnsafeSerializeVertexEdgesClass: " +
        "Serializing took " +
        serializeNanos +
        " ns for " + outputStream.getPos()
        + " bytes " +
        (outputStream.getPos() * 1f *
            Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    Vertex<LongWritable, FloatWritable, DoubleWritable, LongWritable>
        readVertex = instantiateVertex(edgesClass);

    long deserializeNanosStart;
    long deserializeNanos = 0;
    for (int i = 0; i < REPS; ++i) {
      deserializeNanosStart = SystemTime.get().getNanoseconds();
      UnsafeByteArrayInputStream inputStream = new
          UnsafeByteArrayInputStream(
          outputStream.getByteArray(), 0, outputStream.getPos());
      readVertex.readFields(inputStream);
      deserializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          deserializeNanosStart);
    }
    deserializeNanos /= REPS;
    System.out.println("testUnsafeSerializeVertexEdgesClass: " +
        "Deserializing took " +
        deserializeNanos +
        " ns for " + outputStream.getPos() +
        " bytes " +
        (outputStream.getPos() * 1f *
            Time.NS_PER_SECOND / deserializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    assertEquals(vertex.getId(), readVertex.getId());
    assertEquals(vertex.getValue(), readVertex.getValue());
    assertTrue(EdgeIterables.sameEdges(vertex.getEdges(), readVertex.getEdges()));
  }
}
