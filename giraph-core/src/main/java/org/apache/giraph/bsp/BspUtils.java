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

package org.apache.giraph.bsp;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.vertex.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.HashPartitionerFactory;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Help to use the configuration to get the appropriate classes or
 * instantiate them.
 */
public class BspUtils {
  /**
   * Do not construct.
   */
  private BspUtils() { }

  /**
   * Get the user's subclassed {@link GraphPartitionerFactory}.
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param conf Configuration to check
   * @return User's graph partitioner
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  Class<? extends GraphPartitionerFactory<I, V, E, M>>
  getGraphPartitionerClass(Configuration conf) {
    return (Class<? extends GraphPartitionerFactory<I, V, E, M>>)
      conf.getClass(GiraphConstants.GRAPH_PARTITIONER_FACTORY_CLASS,
        HashPartitionerFactory.class,
        GraphPartitionerFactory.class);
  }

  /**
   * Create a user graph partitioner class
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param conf Configuration to check
   * @return Instantiated user graph partitioner class
   */
  @SuppressWarnings("rawtypes")
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  GraphPartitionerFactory<I, V, E, M>
  createGraphPartitioner(Configuration conf) {
    Class<? extends GraphPartitionerFactory<I, V, E, M>>
    graphPartitionerFactoryClass = getGraphPartitionerClass(conf);
    return ReflectionUtils.newInstance(graphPartitionerFactoryClass, conf);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.io.VertexInputFormat}.
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param conf Configuration to check
   * @return User's vertex input format class
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <I extends WritableComparable,
  V extends Writable,
  E extends Writable,
  M extends Writable>
  Class<? extends VertexInputFormat<I, V, E, M>>
  getVertexInputFormatClass(Configuration conf) {
    return (Class<? extends VertexInputFormat<I, V, E, M>>)
      conf.getClass(GiraphConstants.VERTEX_INPUT_FORMAT_CLASS,
        null,
        VertexInputFormat.class);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.io.VertexOutputFormat}.
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param conf Configuration to check
   * @return User's vertex output format class
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <I extends WritableComparable,
  V extends Writable,
  E extends Writable>
  Class<? extends VertexOutputFormat<I, V, E>>
  getVertexOutputFormatClass(Configuration conf) {
    return (Class<? extends VertexOutputFormat<I, V, E>>)
      conf.getClass(GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS,
        null,
        VertexOutputFormat.class);
  }

  /**
   * Create a user vertex output format class
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param conf Configuration to check
   * @return Instantiated user vertex output format class
   */
  @SuppressWarnings("rawtypes")
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable> VertexOutputFormat<I, V, E>
  createVertexOutputFormat(Configuration conf) {
    Class<? extends VertexOutputFormat<I, V, E>> vertexOutputFormatClass =
      getVertexOutputFormatClass(conf);
    return ReflectionUtils.newInstance(vertexOutputFormatClass, conf);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.io.EdgeInputFormat}.
   *
   * @param <I> Vertex id
   * @param <E> Edge data
   * @param conf Configuration to check
   * @return User's edge input format class
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <I extends WritableComparable, E extends Writable>
  Class<? extends EdgeInputFormat<I, E>>
  getEdgeInputFormatClass(Configuration conf) {
    return (Class<? extends EdgeInputFormat<I, E>>)
        conf.getClass(GiraphConstants.EDGE_INPUT_FORMAT_CLASS,
            null,
            EdgeInputFormat.class);
  }

  /**
   * Get the user's subclassed {@link AggregatorWriter}.
   *
   * @param conf Configuration to check
   * @return User's aggregator writer class
   */
  public static Class<? extends AggregatorWriter>
  getAggregatorWriterClass(Configuration conf) {
    return conf.getClass(GiraphConstants.AGGREGATOR_WRITER_CLASS,
        TextAggregatorWriter.class,
        AggregatorWriter.class);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.combiner.Combiner}.
   *
   * @param <I> Vertex id
   * @param <M> Message data
   * @param conf Configuration to check
   * @return User's vertex combiner class
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <I extends WritableComparable, M extends Writable>
  Class<? extends Combiner<I, M>> getCombinerClass(Configuration conf) {
    return (Class<? extends Combiner<I, M>>)
      conf.getClass(GiraphConstants.VERTEX_COMBINER_CLASS,
        null,
        Combiner.class);
  }

  /**
   * Get the user's subclassed VertexResolver.
   *
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param conf Configuration to check
   * @return User's vertex resolver class
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  Class<? extends VertexResolver<I, V, E, M>>
  getVertexResolverClass(Configuration conf) {
    return (Class<? extends VertexResolver<I, V, E, M>>)
      conf.getClass(GiraphConstants.VERTEX_RESOLVER_CLASS,
        DefaultVertexResolver.class,
        VertexResolver.class);
  }

  /**
   * Get the user's subclassed WorkerContext.
   *
   * @param conf Configuration to check
   * @return User's worker context class
   */
  public static Class<? extends WorkerContext>
  getWorkerContextClass(Configuration conf) {
    return (Class<? extends WorkerContext>)
      conf.getClass(GiraphConstants.WORKER_CONTEXT_CLASS,
        DefaultWorkerContext.class,
        WorkerContext.class);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.master.MasterCompute}
   *
   * @param conf Configuration to check
   * @return User's master class
   */
  public static Class<? extends MasterCompute>
  getMasterComputeClass(Configuration conf) {
    return (Class<? extends MasterCompute>)
      conf.getClass(GiraphConstants.MASTER_COMPUTE_CLASS,
        DefaultMasterCompute.class,
        MasterCompute.class);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.vertex.Vertex}
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param conf Configuration to check
   * @return User's vertex class
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable>
  Class<? extends Vertex<I, V, E, M>> getVertexClass(Configuration conf) {
    return (Class<? extends Vertex<I, V, E, M>>)
      conf.getClass(GiraphConstants.VERTEX_CLASS,
        null,
        Vertex.class);
  }

  /**
   * Create a user vertex
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   * @param conf Configuration to check
   * @return Instantiated user vertex
   */
  @SuppressWarnings("rawtypes")
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable> Vertex<I, V, E, M>
  createVertex(Configuration conf) {
    Class<? extends Vertex<I, V, E, M>> vertexClass = getVertexClass(conf);
    Vertex<I, V, E, M> vertex =
      ReflectionUtils.newInstance(vertexClass, conf);
    return vertex;
  }

  /**
   * Get the user's subclassed vertex index class.
   *
   * @param <I> Vertex id
   * @param conf Configuration to check
   * @return User's vertex index class
   */
  @SuppressWarnings("unchecked")
  public static <I extends Writable> Class<I>
  getVertexIdClass(Configuration conf) {
    return (Class<I>) conf.getClass(GiraphConstants.VERTEX_ID_CLASS,
      WritableComparable.class);
  }

  /**
   * Create a user vertex index
   *
   * @param <I> Vertex id
   * @param conf Configuration to check
   * @return Instantiated user vertex index
   */
  @SuppressWarnings("rawtypes")
  public static <I extends WritableComparable>
  I createVertexId(Configuration conf) {
    Class<I> vertexIdClass = getVertexIdClass(conf);
    try {
      return vertexIdClass.newInstance();
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(
        "createVertexId: Failed to instantiate", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
        "createVertexId: Illegally accessed", e);
    }
  }

  /**
   * Get the user's subclassed vertex value class.
   *
   * @param <V> Vertex data
   * @param conf Configuration to check
   * @return User's vertex value class
   */
  @SuppressWarnings("unchecked")
  public static <V extends Writable> Class<V>
  getVertexValueClass(Configuration conf) {
    return (Class<V>) conf.getClass(GiraphConstants.VERTEX_VALUE_CLASS,
      Writable.class);
  }

  /**
   * Create a user vertex value
   *
   * @param <V> Vertex data
   * @param conf Configuration to check
   * @return Instantiated user vertex value
   */
  @SuppressWarnings("unchecked")
  public static <V extends Writable> V
  createVertexValue(Configuration conf) {
    Class<V> vertexValueClass = getVertexValueClass(conf);
    if (vertexValueClass == NullWritable.class) {
      return (V) NullWritable.get();
    } else {
      try {
        return vertexValueClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(
          "createVertexValue: Failed to instantiate", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(
          "createVertexValue: Illegally accessed", e);
      }
    }
  }

  /**
   * Get the user's subclassed edge value class.
   *
   * @param <E> Edge data
   * @param conf Configuration to check
   * @return User's vertex edge value class
   */
  @SuppressWarnings("unchecked")
  public static <E extends Writable> Class<E>
  getEdgeValueClass(Configuration conf) {
    return (Class<E>) conf.getClass(GiraphConstants.EDGE_VALUE_CLASS,
      Writable.class);
  }

  /**
   * Get the user's subclassed vertex message value class.
   *
   * @param <M> Message data
   * @param conf Configuration to check
   * @return User's vertex message value class
   */
  @SuppressWarnings("unchecked")
  public static <M extends Writable> Class<M>
  getMessageValueClass(Configuration conf) {
    return (Class<M>) conf.getClass(GiraphConstants.MESSAGE_VALUE_CLASS,
      Writable.class);
  }
}
