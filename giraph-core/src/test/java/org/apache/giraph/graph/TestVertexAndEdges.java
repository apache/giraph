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
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.utils.DynamicChannelBufferInputStream;
import org.apache.giraph.utils.DynamicChannelBufferOutputStream;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.NoOpComputation;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link Vertex} functionality across the provided {@link org.apache.giraph.edge.OutEdges}
 * classes.
 */
public class TestVertexAndEdges {
  /** Number of repetitions. */
  public static final int REPS = 100;
  /** {@link org.apache.giraph.edge.OutEdges} classes to be tested. */
  private Collection<Class<? extends OutEdges>> edgesClasses =
      Lists.newArrayList();

  /**
   * Dummy concrete vertex.
   */
  public static class TestComputation extends NoOpComputation<LongWritable,
      FloatWritable, DoubleWritable, LongWritable> { }

  /**
   * A basic {@link org.apache.giraph.edge.OutEdges} implementation that doesn't provide any
   * special functionality. Used to test the default implementations of
   * Vertex#getEdgeValue(), Vertex#getMutableEdges(), etc.
   */
  public static class TestOutEdges
      implements OutEdges<LongWritable, DoubleWritable> {
    private List<Edge<LongWritable, DoubleWritable>> edgeList;


    @Override
    public void initialize(Iterable<Edge<LongWritable, DoubleWritable>> edges) {
      this.edgeList = Lists.newArrayList(edges);
    }

    @Override
    public void initialize(int capacity) {
      this.edgeList = Lists.newArrayListWithCapacity(capacity);
    }

    @Override
    public void initialize() {
      this.edgeList = Lists.newArrayList();
    }

    @Override
    public void add(Edge<LongWritable, DoubleWritable> edge) {
      edgeList.add(edge);
    }

    @Override
    public void remove(LongWritable targetVertexId) {
      for (Iterator<Edge<LongWritable, DoubleWritable>> edges =
               edgeList.iterator(); edges.hasNext();) {
        Edge<LongWritable, DoubleWritable> edge = edges.next();
        if (edge.getTargetVertexId().equals(targetVertexId)) {
          edges.remove();
        }
      }
    }

    @Override
    public int size() {
      return edgeList.size();
    }

    @Override
    public Iterator<Edge<LongWritable, DoubleWritable>> iterator() {
      return edgeList.iterator();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(edgeList.size());
      for (Edge<LongWritable, DoubleWritable> edge : edgeList) {
        edge.getTargetVertexId().write(out);
        edge.getValue().write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int numEdges = in.readInt();
      initialize(numEdges);
      for (int i = 0; i < numEdges; ++i) {
        Edge<LongWritable, DoubleWritable> edge = EdgeFactory.createReusable(
            new LongWritable(), new DoubleWritable());
        WritableUtils.readEdge(in, edge);
        edgeList.add(edge);
      }
    }
  }

  @Before
  public void setUp() {
    edgesClasses.add(TestOutEdges.class);
    edgesClasses.add(ByteArrayEdges.class);
    edgesClasses.add(ArrayListEdges.class);
    edgesClasses.add(HashMapEdges.class);
    edgesClasses.add(HashMultimapEdges.class);
    edgesClasses.add(LongDoubleArrayEdges.class);
    edgesClasses.add(LongDoubleHashMapEdges.class);
  }

  protected Vertex<LongWritable, FloatWritable, DoubleWritable>
  instantiateVertex(Class<? extends OutEdges> edgesClass) {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    giraphConfiguration.setComputationClass(TestComputation.class);
    giraphConfiguration.setOutEdgesClass(edgesClass);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration =
        new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    return immutableClassesGiraphConfiguration.createVertex();
  }

  /**
   * Test vertex instantiation, initialization, and updating the vertex value.
   */
  @Test
  public void testVertexIdAndValue() {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        instantiateVertex(ArrayListEdges.class);
    assertNotNull(vertex);
    vertex.initialize(new LongWritable(7), new FloatWritable(3.0f));
    assertEquals(7, vertex.getId().get());
    assertEquals(3.0f, vertex.getValue().get(), 0d);
    vertex.setValue(new FloatWritable(5.5f));
    assertEquals(5.5f, vertex.getValue().get(), 0d);
  }

  public static OutEdges
  instantiateOutEdges(Class<? extends OutEdges> edgesClass) {
    GiraphConfiguration giraphConfiguration = new GiraphConfiguration();
    // Needed to extract type arguments in ReflectionUtils.
    giraphConfiguration.setComputationClass(TestComputation.class);
    giraphConfiguration.setOutEdgesClass(edgesClass);
    ImmutableClassesGiraphConfiguration immutableClassesGiraphConfiguration =
        new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    return immutableClassesGiraphConfiguration.createOutEdges();
  }

  /**
   * Test the provided {@link org.apache.giraph.edge.OutEdges} implementations for instantiation,
   * initialization, edge addition, and edge removal.
   */
  @Test
  public void testEdges() {
    for (Class<? extends OutEdges> edgesClass : edgesClasses) {
      testEdgesClass(edgesClass);
    }
  }

  private void testEdgesClass(Class<? extends OutEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        instantiateVertex(edgesClass);
    OutEdges<LongWritable, DoubleWritable> outEdges =
        instantiateOutEdges(edgesClass);
    assertNotNull(outEdges);

    List<Edge<LongWritable, DoubleWritable>> edges = Lists.newLinkedList();
    for (int i = 1000; i > 0; --i) {
      edges.add(EdgeFactory.create(new LongWritable(i),
          new DoubleWritable(i * 2.0)));
    }

    outEdges.initialize(edges);
    vertex.initialize(new LongWritable(1), new FloatWritable(1), outEdges);

    assertEquals(20.0, vertex.getEdgeValue(new LongWritable(10)).get(), 0.0);

    assertEquals(1000, vertex.getNumEdges());
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      assertEquals(edge.getTargetVertexId().get() * 2.0d,
          edge.getValue().get(), 0d);
    }
    vertex.removeEdges(new LongWritable(500));
    assertEquals(999, vertex.getNumEdges());
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      assertTrue(edge.getTargetVertexId().get() != 500);
    }

    vertex.setEdgeValue(new LongWritable(10), new DoubleWritable(33.0));
    assertEquals(33.0, vertex.getEdgeValue(new LongWritable(10)).get(), 0);
  }

  /**
   * Test in-place edge mutations via the iterable returned by {@link
   * Vertex#getMutableEdges()}.
   */
  @Test
  public void testMutateEdges() {
    for (Class<? extends OutEdges> edgesClass : edgesClasses) {
      testMutateEdgesClass(edgesClass);
    }
  }

  private void testMutateEdgesClass(Class<? extends OutEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        instantiateVertex(edgesClass);
    OutEdges<LongWritable, DoubleWritable> outEdges =
        instantiateOutEdges(edgesClass);

    outEdges.initialize();
    vertex.initialize(new LongWritable(0), new FloatWritable(0), outEdges);

    // Add 10 edges with id i, value i for i = 0..9
    for (int i = 0; i < 10; ++i) {
      vertex.addEdge(EdgeFactory.create(
          new LongWritable(i), new DoubleWritable(i)));
    }

    // Use the mutable iterable to multiply each edge value by 2
    for (MutableEdge<LongWritable, DoubleWritable> edge :
        vertex.getMutableEdges()) {
      edge.setValue(new DoubleWritable(edge.getValue().get() * 2));
    }

    // We should still have 10 edges
    assertEquals(10, vertex.getNumEdges());
    // The edge values should now be double the ids
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      long id = edge.getTargetVertexId().get();
      double value = edge.getValue().get();
      assertEquals(id * 2, value, 0);
    }

    // Use the mutable iterator to remove edges with even id
    Iterator<MutableEdge<LongWritable, DoubleWritable>> edgeIt =
        vertex.getMutableEdges().iterator();
    while (edgeIt.hasNext()) {
      if (edgeIt.next().getTargetVertexId().get() % 2 == 0) {
        edgeIt.remove();
      }
    }

    // We should now have 5 edges
    assertEquals(5, vertex.getNumEdges());
    // The edge ids should be all odd
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      assertEquals(1, edge.getTargetVertexId().get() % 2);
    }

    // Breaking iteration early should not make us lose edges.
    // This version uses repeated calls to next():
    Iterator<MutableEdge<LongWritable, DoubleWritable>> it =
        vertex.getMutableEdges().iterator();
    it.next();
    it.next();
    assertEquals(5, vertex.getNumEdges());

    // This version uses a for-each loop, and the break statement:
    int i = 2;
    for (MutableEdge<LongWritable, DoubleWritable> edge :
        vertex.getMutableEdges()) {
      if (i-- == 0) {
        break;
      }
    }
    assertEquals(5, vertex.getNumEdges());

    // This version uses a normal, immutable iterable:
    i = 2;
    for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
      if (i-- == 0) {
        break;
      }
    }
    assertEquals(5, vertex.getNumEdges());

    // Calling size() during iteration shouldn't modify the data structure.
    int iterations = 0;
    for (MutableEdge<LongWritable, DoubleWritable> edge : vertex.getMutableEdges()) {
      edge.setValue(new DoubleWritable(3));
      assertEquals(5, vertex.getNumEdges());
      ++iterations;
    }
    assertEquals(5, vertex.getNumEdges());
    assertEquals(5, iterations);

    // If we remove an edge after calling next(), size() should return the
    // correct number of edges.
    it = vertex.getMutableEdges().iterator();
    it.next();
    it.remove();
    assertEquals(4, vertex.getNumEdges());
    it.next();
    it.remove();
    assertEquals(3, vertex.getNumEdges());
  }

  /**
   * Test {@link Vertex} and {@link org.apache.giraph.edge.OutEdges} serialization.
   * @throws IOException
   */
  @Test
  public void testSerialize() throws IOException {
    for (Class<? extends OutEdges> edgesClass : edgesClasses) {
      testSerializeOutEdgesClass(edgesClass);
      testDynamicChannelBufferSerializeOutEdgesClass(edgesClass);
      testUnsafeSerializeOutEdgesClass(edgesClass);
    }
  }

  protected Vertex<LongWritable, FloatWritable, DoubleWritable>
  buildVertex(Class<? extends OutEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        instantiateVertex(edgesClass);
    OutEdges<LongWritable, DoubleWritable> outEdges =
        instantiateOutEdges(edgesClass);

    int edgesCount = 200;
    List<Edge<LongWritable, DoubleWritable>> edges =
        Lists.newArrayListWithCapacity(edgesCount);
    for (int i = edgesCount; i > 0; --i) {
      edges.add(EdgeFactory.create(new LongWritable(i),
          new DoubleWritable(i * 2.0)));
    }

    outEdges.initialize(edges);
    vertex.initialize(new LongWritable(2), new FloatWritable(3.0f),
        outEdges);
    return vertex;
  }

  private void testSerializeOutEdgesClass(
      Class<? extends OutEdges> edgesClass) {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        buildVertex(edgesClass);

    long serializeNanosStart;
    long serializeNanos = 0;
    byte[] byteArray = null;
    for (int i = 0; i < REPS; ++i) {
      serializeNanosStart = SystemTime.get().getNanoseconds();
      byteArray = WritableUtils.writeVertexToByteArray(
          vertex, false, vertex.getConf());
      serializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          serializeNanosStart);
    }
    serializeNanos /= REPS;
    System.out.println("testSerialize: Serializing took " +
        serializeNanos + " ns for " + byteArray.length + " bytes " +
        (byteArray.length * 1f * Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    Vertex<LongWritable, FloatWritable, DoubleWritable>
        readVertex = buildVertex(edgesClass);
    
    long deserializeNanosStart;
    long deserializeNanos = 0;
    for (int i = 0; i < REPS; ++i) {
      deserializeNanosStart = SystemTime.get().getNanoseconds();
      WritableUtils.reinitializeVertexFromByteArray(byteArray, readVertex, false, 
          readVertex.getConf());
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

  private void testDynamicChannelBufferSerializeOutEdgesClass(
      Class<? extends OutEdges> edgesClass)
      throws IOException {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        buildVertex(edgesClass);

    long serializeNanosStart;
    long serializeNanos = 0;
    DynamicChannelBufferOutputStream outputStream = null;
    for (int i = 0; i <
        REPS; ++i) {
      serializeNanosStart = SystemTime.get().getNanoseconds();
      outputStream =
          new DynamicChannelBufferOutputStream(32);
      WritableUtils.writeVertexToDataOutput(outputStream, vertex, vertex.getConf());
      serializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          serializeNanosStart);
    }
    serializeNanos /= REPS;
    System.out.println("testDynamicChannelBufferSerializeOutEdgesClass: " +
        "Serializing took " + serializeNanos + " ns for " +
        outputStream.getDynamicChannelBuffer().writerIndex() + " bytes " +
        (outputStream.getDynamicChannelBuffer().writerIndex() * 1f *
            Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    Vertex<LongWritable, FloatWritable, DoubleWritable>
        readVertex = buildVertex(edgesClass);

    long deserializeNanosStart;
    long deserializeNanos = 0;
    for (int i = 0; i < REPS; ++i) {
      deserializeNanosStart = SystemTime.get().getNanoseconds();
      DynamicChannelBufferInputStream inputStream = new
          DynamicChannelBufferInputStream(
          outputStream.getDynamicChannelBuffer());
      WritableUtils.reinitializeVertexFromDataInput(
          inputStream, readVertex, readVertex.getConf());
      deserializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          deserializeNanosStart);
      outputStream.getDynamicChannelBuffer().readerIndex(0);
    }
    deserializeNanos /= REPS;
    System.out.println("testDynamicChannelBufferSerializeOutEdgesClass: " +
        "Deserializing took " + deserializeNanos + " ns for " +
        outputStream.getDynamicChannelBuffer().writerIndex() + " bytes " +
        (outputStream.getDynamicChannelBuffer().writerIndex() * 1f *
            Time.NS_PER_SECOND / deserializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    assertEquals(vertex.getId(), readVertex.getId());
    assertEquals(vertex.getValue(), readVertex.getValue());
    assertTrue(EdgeIterables.sameEdges(vertex.getEdges(), readVertex.getEdges()));
  }

  private void testUnsafeSerializeOutEdgesClass(
      Class<? extends OutEdges> edgesClass)
      throws IOException {
    Vertex<LongWritable, FloatWritable, DoubleWritable> vertex =
        buildVertex(edgesClass);

    long serializeNanosStart;
    long serializeNanos = 0;
    UnsafeByteArrayOutputStream outputStream = null;
    for (int i = 0; i <
        REPS; ++i) {
      serializeNanosStart = SystemTime.get().getNanoseconds();
      outputStream =
          new UnsafeByteArrayOutputStream(32);
      WritableUtils.writeVertexToDataOutput(outputStream, vertex, vertex.getConf());
      serializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          serializeNanosStart);
    }
    serializeNanos /= REPS;
    System.out.println("testUnsafeSerializeOutEdgesClass: " +
        "Serializing took " +
        serializeNanos +
        " ns for " + outputStream.getPos()
        + " bytes " +
        (outputStream.getPos() * 1f *
            Time.NS_PER_SECOND / serializeNanos) +
        " bytes / sec for " + edgesClass.getName());

    Vertex<LongWritable, FloatWritable, DoubleWritable>
        readVertex = buildVertex(edgesClass);

    long deserializeNanosStart;
    long deserializeNanos = 0;
    for (int i = 0; i < REPS; ++i) {
      deserializeNanosStart = SystemTime.get().getNanoseconds();
      UnsafeByteArrayInputStream inputStream = new
          UnsafeByteArrayInputStream(
          outputStream.getByteArray(), 0, outputStream.getPos());
      WritableUtils.reinitializeVertexFromDataInput(
          inputStream, readVertex, readVertex.getConf());
      deserializeNanos += Times.getNanosecondsSince(SystemTime.get(),
          deserializeNanosStart);
    }
    deserializeNanos /= REPS;
    System.out.println("testUnsafeSerializeOutEdgesClass: " +
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
