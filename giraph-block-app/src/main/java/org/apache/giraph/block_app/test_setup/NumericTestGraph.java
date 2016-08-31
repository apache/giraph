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
package org.apache.giraph.block_app.test_setup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.function.Function;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Wraps TestGraph to allow using numbers to create and inspect the graph,
 * instead of needing to have actual Writable values, which don't have
 * auto-boxing.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
public class NumericTestGraph<I extends WritableComparable,
    V extends Writable,
    E extends Writable> {

  private static final Logger LOG = Logger.getLogger(NumericTestGraph.class);

  private final TestGraph<I, V, E> testGraph;
  private final Function<Number, I> numberToVertexId;
  private final Function<Number, V> numberToVertexValue;
  private final Function<Number, E> numberToEdgeValue;

  public NumericTestGraph(TestGraph<I, V, E> testGraph) {
    this.testGraph = testGraph;
    numberToVertexId =
        numericConvForType(testGraph.getConf().getVertexIdClass());
    numberToVertexValue =
        numericConvForType(testGraph.getConf().getVertexValueClass());
    numberToEdgeValue =
        numericConvForType(testGraph.getConf().getEdgeValueClass());
    Preconditions.checkState(this.numberToVertexId != null);
  }

  public NumericTestGraph(GiraphConfiguration conf) {
    this(new TestGraph<I, V, E>(conf));
  }

  public ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
    return testGraph.getConf();
  }

  public TestGraph<I, V, E> getTestGraph() {
    return testGraph;
  }


  /**
   * Get Vertex for a given id.
   */
  public Vertex<I, V, E> getVertex(Number vertexId) {
    return testGraph.getVertex(numberToVertexId(vertexId));
  }

  /**
   * Get Vertex Value for a given id.
   */
  public V getValue(Number vertexId) {
    return testGraph.getVertex(numberToVertexId(vertexId)).getValue();
  }

  /**
   * Get number of vertices in the graph
   */
  public int getVertexCount() {
    return testGraph.getVertexCount();
  }

  /**
   * Add Vertex with a given id to the graph, initializing it to
   * default vertex value and no edges.
   */
  public void addVertex(Number vertexId) {
    addVertex(vertexId, (Number) null);
  }

  /**
   * Add Vertex with a given id and a given Vertex Value to the graph,
   * initializing it to have no edges.
   */
  public void addVertex(Number vertexId, Number vertexValue) {
    addVertex(vertexId, vertexValue, null);
  }

  /**
   * Add Vertex with a given id and a given Vertex Value to the graph,
   * with listed outgoing edges, all initialized to same provided
   * {@code edgeValue}.
   */
  public void addVertex(Number vertexId, Number vertexValue,
      Number edgeValue, Number... outEdges) {
    Vertex<I, V, E> vertex = makeVertex(
        vertexId, vertexValue, edgeValue, outEdges);
    testGraph.addVertex(vertex);
  }

  /**
   * Add Vertex with a given id and a given Vertex Value to the graph,
   * initializing it to have no edges.
   */
  public void addVertex(Number vertexId, V vertexValue) {
    addVertex(vertexId, vertexValue, null);
  }

  /**
   * Add Vertex with a given id and a given Vertex Value to the graph,
   * with listed outgoing edges, all initialized to same provided
   * {@code edgeSupplier}.
   */
  public void addVertex(Number vertexId, V vertexValue,
      Supplier<E> edgeSupplier, Number... outEdges) {
    Vertex<I, V, E> vertex = makeVertex(
        vertexId, vertexValue, edgeSupplier, outEdges);
    testGraph.addVertex(vertex);
  }

  /**
   * Add Edge to the graph with default Edge Value, by adding it to
   * outEdges of {@code fromVertex}, potentially creating {@code fromVertex}
   * if it doesn't exist.
   */
  public void addEdge(Number fromVertex, Number toVertex) {
    addEdge(fromVertex, toVertex, (Number) null);
  }

  /**
   * Add Edge to the graph with provided Edge Value, by adding it to
   * outEdges of {@code fromVertex}, potentially creating {@code fromVertex}
   * if it doesn't exist.
   */
  public void addEdge(Number fromVertex, Number toVertex, Number edgeValue) {
    testGraph.addEdge(
        numberToVertexId(fromVertex),
        numberToVertexId(toVertex),
        numberToEdgeValue(edgeValue));
  }

  /**
   * Add Edge to the graph with provided Edge Value, by adding it to
   * outEdges of {@code fromVertex}, potentially creating {@code fromVertex}
   * if it doesn't exist.
   */
  public void addEdge(Number fromVertex, Number toVertex, E edgeValue) {
    testGraph.addEdge(
        numberToVertexId(fromVertex),
        numberToVertexId(toVertex),
        edgeValueOrCreate(edgeValue));
  }

  /**
   * Add symmetric Edge to the graph with default Edge Value, by adding it to
   * outEdges of vertices on both ends, potentially creating them both,
   * if they don't exist.
   */
  public void addSymmetricEdge(Number fromVertex, Number toVertex) {
    addEdge(fromVertex, toVertex);
    addEdge(toVertex, fromVertex);
  }

  /**
   * Add symmetric Edge to the graph with provided Edge Value, by adding it to
   * outEdges of vertices on both ends, potentially creating them both,
   * if they don't exist.
   */
  public void addSymmetricEdge(
      Number fromVertex, Number toVertex, Number edgeValue) {
    addEdge(fromVertex, toVertex, edgeValue);
    addEdge(toVertex, fromVertex, edgeValue);
  }

  /**
   * Add symmetric Edge to the graph with provided Edge Value, by adding it to
   * outEdges of vertices on both ends, potentially creating them both,
   * if they don't exist.
   */
  public void addSymmetricEdge(Number vertexId, Number toVertex, E edgeValue) {
    addEdge(vertexId, toVertex, edgeValue);
    addEdge(toVertex, vertexId, edgeValue);
  }

  /**
   * Creates a new Vertex object, without adding it into the graph.
   *
   * This function is safe to call from multiple threads at the same time,
   * and then synchronize only on actual addition of Vertex to the graph
   * itself.
   */
  public Vertex<I, V, E> makeVertex(
      Number vertexId, V vertexValue,
      Entry<? extends Number, ? extends Number>... edges) {
    Vertex<I, V, E> vertex = getConf().createVertex();
    List<Edge<I, E>> edgesList = new ArrayList<>();

    int i = 0;
    for (Entry<? extends Number, ? extends Number> edge: edges) {
      edgesList.add(EdgeFactory.create(
        numberToVertexId(edge.getKey()),
        numberToEdgeValue(edge.getValue())));
      i++;
    }
    vertex.initialize(
            numberToVertexId(vertexId),
            vertexValue != null ?
              vertexValue : getConf().createVertexValue(),
            edgesList);
    return vertex;
  }

  /**
   * Creates a new Vertex object, without adding it into the graph.
   *
   * This function is safe to call from multiple threads at the same time,
   * and then synchronize only on actual addition of Vertex to the graph
   * itself.
   */
  public Vertex<I, V, E> makeVertex(
      Number vertexId, V vertexValue,
      Supplier<E> edgeSupplier, Number... edges) {
    Vertex<I, V, E> vertex = getConf().createVertex();

    List<Edge<I, E>> edgesList = new ArrayList<>();
    for (Number edge: edges) {
      edgesList.add(
          EdgeFactory.create(numberToVertexId.apply(edge),
          edgeSupplier != null ?
            edgeSupplier.get() : getConf().createEdgeValue()));
    }

    vertex.initialize(
        numberToVertexId.apply(vertexId),
        vertexValue != null ?
          vertexValue : getConf().createVertexValue(),
        edgesList);
    return vertex;
  }

  /**
   * Creates a new Vertex object, without adding it into the graph.
   *
   * This function is safe to call from multiple threads at the same time,
   * and then synchronize only on actual addition of Vertex to the graph
   * itself.
   */
  public Vertex<I, V, E> makeVertex(
      Number vertexId, Number value,
      Number edgeValue, Number... edges) {
    Vertex<I, V, E> vertex = getConf().createVertex();

    List<Edge<I, E>> edgesList = new ArrayList<>();
    for (Number edge: edges) {
      edgesList.add(
          EdgeFactory.create(numberToVertexId.apply(edge),
          numberToEdgeValue(edgeValue)));
    }

    vertex.initialize(
        numberToVertexId.apply(vertexId),
        numberToVertexValue(value),
        edgesList);
    return vertex;
  }

  public I numberToVertexId(Number value) {
    return numberToVertexId.apply(value);
  }

  public V numberToVertexValue(Number value) {
    return value != null ?
      numberToVertexValue.apply(value) : getConf().createVertexValue();
  }

  public E numberToEdgeValue(Number edgeValue) {
    return edgeValue != null ?
      numberToEdgeValue.apply(edgeValue) : getConf().createEdgeValue();
  }

  public E edgeValueOrCreate(E edgeValue) {
    return edgeValue != null ? edgeValue : getConf().createEdgeValue();
  }


  public Vertex<I, V, E> createVertex() {
    return getConf().createVertex();
  }

  public void initializeVertex(
          Vertex<I, V, E> v, I id, Supplier<V> valueSupplier,
          List<Edge<I, E>> edgesList) {
    v.initialize(
            id,
            valueSupplier != null ?
              valueSupplier.get() : getConf().createVertexValue(),
            edgesList != null ? edgesList : new ArrayList<Edge<I, E>>());
  }

  @Override
  public String toString() {
    return testGraph.toString();
  }


  private static Function<Number, IntWritable> numberToInt() {
    return new Function<Number, IntWritable>() {
      @Override
      public IntWritable apply(Number input) {
        return new IntWritable(input.intValue());
      }
    };
  }

  private static Function<Number, LongWritable> numberToLong() {
    return new Function<Number, LongWritable>() {
      @Override
      public LongWritable apply(Number input) {
        return new LongWritable(input.longValue());
      }
    };
  }

  private static Function<Number, DoubleWritable> numberToDouble() {
    return new Function<Number, DoubleWritable>() {
      @Override
      public DoubleWritable apply(Number input) {
        return new DoubleWritable(input.doubleValue());
      }
    };
  }

  private static Function<Number, FloatWritable> numberToFloat() {
    return new Function<Number, FloatWritable>() {
      @Override
      public FloatWritable apply(Number input) {
        return new FloatWritable(input.floatValue());
      }
    };
  }

  private static <T> Function<Number, T> numericConvForType(Class<T> type) {
    if (type.equals(LongWritable.class)) {
      return (Function) numberToLong();
    } else if (type.equals(IntWritable.class)) {
      return (Function) numberToInt();
    } else if (type.equals(DoubleWritable.class)) {
      return (Function) numberToDouble();
    } else if (type.equals(FloatWritable.class)) {
      return (Function) numberToFloat();
    } else {
      return null;
    }
  }
}
