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

package org.apache.giraph;

import java.util.List;
import org.apache.giraph.graph.AggregatorWriter;
import org.apache.giraph.graph.Combiner;
import org.apache.giraph.graph.DefaultMasterCompute;
import org.apache.giraph.graph.DefaultWorkerContext;
import org.apache.giraph.graph.EdgeInputFormat;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.MasterCompute;
import org.apache.giraph.graph.TextAggregatorWriter;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.graph.partition.GraphPartitionerFactory;
import org.apache.giraph.graph.partition.HashPartitionerFactory;
import org.apache.giraph.graph.partition.MasterGraphPartitioner;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.partition.SimplePartition;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

/**
 * The classes set here are immutable, the remaining configuration is mutable.
 * Classes are immutable and final to provide the best performance for
 * instantiation.  Everything is thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class ImmutableClassesGiraphConfiguration<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> extends
    GiraphConfiguration {
  /** Vertex class - cached for fast access */
  private final Class<? extends Vertex<I, V, E, M>> vertexClass;
  /** Vertex id class - cached for fast access */
  private final Class<I> vertexIdClass;
  /** Vertex value class - cached for fast access */
  private final Class<V> vertexValueClass;
  /** Edge value class - cached for fast access */
  private final Class<E> edgeValueClass;
  /** Message value class - cached for fast access */
  private final Class<M> messageValueClass;

  /** Graph partitioner factory class - cached for fast access */
  private final Class<? extends GraphPartitionerFactory<I, V, E, M>>
  graphPartitionerFactoryClass;
  /** Master graph partitioner - cached for fast access */
  private final MasterGraphPartitioner<I, V, E, M> masterGraphPartitioner;

  /** Vertex input format class - cached for fast access */
  private final Class<? extends VertexInputFormat<I, V, E, M>>
  vertexInputFormatClass;
  /** Vertex output format class - cached for fast access */
  private final Class<? extends VertexOutputFormat<I, V, E>>
  vertexOutputFormatClass;
  /** Edge input format class - cached for fast access */
  private final Class<? extends EdgeInputFormat<I, E>>
  edgeInputFormatClass;


  /** Aggregator writer class - cached for fast access */
  private final Class<? extends AggregatorWriter> aggregatorWriterClass;
  /** Combiner class - cached for fast access */
  private final Class<? extends Combiner<I, M>> combinerClass;

  /** Vertex resolver class - cached for fast access */
  private final Class<? extends VertexResolver<I, V, E, M>>
  vertexResolverClass;
  /** Worker context class - cached for fast access */
  private final Class<? extends WorkerContext> workerContextClass;
  /** Master compute class - cached for fast access */
  private final Class<? extends MasterCompute> masterComputeClass;

  /** Partition class - cached for fast accesss */
  private final Class<? extends Partition<I, V, E, M>> partitionClass;

  /**
   * Use unsafe serialization? Cached for fast access to instantiate the
   * extended data input/output classes
   */
  private final boolean useUnsafeSerialization;

  /**
   * Constructor.  Takes the configuration and then gets the classes out of
   * them for Giraph
   *
   * @param conf Configuration
   */
  public ImmutableClassesGiraphConfiguration(Configuration conf) {
    super(conf);
    // set pre-validated generic parameter types into Configuration
    vertexClass = (Class<? extends Vertex<I, V, E, M>>)
        conf.getClass(VERTEX_CLASS, null, Vertex.class);
    List<Class<?>> classList =
        org.apache.giraph.utils.ReflectionUtils.<Vertex>getTypeArguments(
            Vertex.class, vertexClass);
    vertexIdClass = (Class<I>) classList.get(0);
    vertexValueClass = (Class<V>) classList.get(1);
    edgeValueClass = (Class<E>) classList.get(2);
    messageValueClass = (Class<M>) classList.get(3);

    graphPartitionerFactoryClass =
        (Class<? extends GraphPartitionerFactory<I, V, E, M>>)
        conf.getClass(GRAPH_PARTITIONER_FACTORY_CLASS,
            HashPartitionerFactory.class,
            GraphPartitionerFactory.class);
    masterGraphPartitioner =
        (MasterGraphPartitioner<I, V, E, M>)
            createGraphPartitioner().createMasterGraphPartitioner();

    vertexInputFormatClass = (Class<? extends VertexInputFormat<I, V, E, M>>)
        conf.getClass(VERTEX_INPUT_FORMAT_CLASS,
        null, VertexInputFormat.class);
    vertexOutputFormatClass = (Class<? extends VertexOutputFormat<I, V, E>>)
        conf.getClass(VERTEX_OUTPUT_FORMAT_CLASS,
        null, VertexOutputFormat.class);
    edgeInputFormatClass = (Class<? extends EdgeInputFormat<I, E>>)
        conf.getClass(EDGE_INPUT_FORMAT_CLASS,
        null, EdgeInputFormat.class);

    aggregatorWriterClass = conf.getClass(AGGREGATOR_WRITER_CLASS,
        TextAggregatorWriter.class, AggregatorWriter.class);
    combinerClass = (Class<? extends Combiner<I, M>>)
        conf.getClass(VERTEX_COMBINER_CLASS, null, Combiner.class);
    vertexResolverClass = (Class<? extends VertexResolver<I, V, E, M>>)
        conf.getClass(VERTEX_RESOLVER_CLASS,
        VertexResolver.class, VertexResolver.class);
    workerContextClass = conf.getClass(WORKER_CONTEXT_CLASS,
        DefaultWorkerContext.class, WorkerContext.class);
    masterComputeClass =  conf.getClass(MASTER_COMPUTE_CLASS,
        DefaultMasterCompute.class, MasterCompute.class);

    partitionClass = (Class<? extends Partition<I, V, E, M>>)
        conf.getClass(PARTITION_CLASS, SimplePartition.class);

    useUnsafeSerialization = getBoolean(USE_UNSAFE_SERIALIZATION,
        USE_UNSAFE_SERIALIZATION_DEFAULT);
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.graph.partition.GraphPartitionerFactory}.
   *
   * @return User's graph partitioner
   */
  public Class<? extends GraphPartitionerFactory<I, V, E, M>>
  getGraphPartitionerClass() {
    return graphPartitionerFactoryClass;
  }

  /**
   * Create a user graph partitioner class
   *
   * @return Instantiated user graph partitioner class
   */
  public GraphPartitionerFactory<I, V, E, M> createGraphPartitioner() {
    return ReflectionUtils.newInstance(graphPartitionerFactoryClass, this);
  }

  /**
   * Create a user graph partitioner partition stats class
   *
   * @return Instantiated user graph partition stats class
   */
  public PartitionStats createGraphPartitionStats() {
    return masterGraphPartitioner.createPartitionStats();
  }

  /**
   * Does the job have a {@link VertexInputFormat}?
   *
   * @return True iff a {@link VertexInputFormat} has been specified.
   */
  public boolean hasVertexInputFormat() {
    return vertexInputFormatClass != null;
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.graph.VertexInputFormat}.
   *
   * @return User's vertex input format class
   */
  public Class<? extends VertexInputFormat<I, V, E, M>>
  getVertexInputFormatClass() {
    return vertexInputFormatClass;
  }

  /**
   * Create a user vertex input format class
   *
   * @return Instantiated user vertex input format class
   */
  public VertexInputFormat<I, V, E, M>
  createVertexInputFormat() {
    return ReflectionUtils.newInstance(vertexInputFormatClass, this);
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.graph.VertexOutputFormat}.
   *
   * @return User's vertex output format class
   */
  public Class<? extends VertexOutputFormat<I, V, E>>
  getVertexOutputFormatClass() {
    return vertexOutputFormatClass;
  }

  /**
   * Create a user vertex output format class
   *
   * @return Instantiated user vertex output format class
   */
  @SuppressWarnings("rawtypes")
  public VertexOutputFormat<I, V, E> createVertexOutputFormat() {
    return ReflectionUtils.newInstance(vertexOutputFormatClass, this);
  }

  /**
   * Does the job have an {@link EdgeInputFormat}?
   *
   * @return True iff an {@link EdgeInputFormat} has been specified.
   */
  public boolean hasEdgeInputFormat() {
    return edgeInputFormatClass != null;
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.graph.EdgeInputFormat}.
   *
   * @return User's edge input format class
   */
  public Class<? extends EdgeInputFormat<I, E>> getEdgeInputFormatClass() {
    return edgeInputFormatClass;
  }

  /**
   * Create a user edge input format class
   *
   * @return Instantiated user edge input format class
   */
  public EdgeInputFormat<I, E> createEdgeInputFormat() {
    return ReflectionUtils.newInstance(edgeInputFormatClass, this);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.graph.AggregatorWriter}.
   *
   * @return User's aggregator writer class
   */
  public Class<? extends AggregatorWriter> getAggregatorWriterClass() {
    return aggregatorWriterClass;
  }

  /**
   * Create a user aggregator output format class
   *
   * @return Instantiated user aggregator writer class
   */
  public AggregatorWriter createAggregatorWriter() {
    return ReflectionUtils.newInstance(aggregatorWriterClass, this);
  }

  /**
   * Create a user combiner class
   *
   * @return Instantiated user combiner class
   */
  @SuppressWarnings("rawtypes")
  public Combiner<I, M> createCombiner() {
    return ReflectionUtils.newInstance(combinerClass, this);
  }

  /**
   * Check if user set a combiner
   *
   * @return True iff user set a combiner class
   */
  public boolean useCombiner() {
    return combinerClass != null;
  }

  /**
   * Get the user's subclassed VertexResolver.
   *
   * @return User's vertex resolver class
   */
  public Class<? extends VertexResolver<I, V, E, M>> getVertexResolverClass() {
    return vertexResolverClass;
  }

  /**
   * Create a user vertex revolver
   *
   * @param graphState State of the graph from the worker
   * @return Instantiated user vertex resolver
   */
  @SuppressWarnings("rawtypes")
  public VertexResolver<I, V, E, M> createVertexResolver(
                       GraphState<I, V, E, M> graphState) {
    VertexResolver<I, V, E, M> resolver =
        ReflectionUtils.newInstance(vertexResolverClass, this);
    resolver.setGraphState(graphState);
    return resolver;
  }

  /**
   * Get the user's subclassed WorkerContext.
   *
   * @return User's worker context class
   */
  public Class<? extends WorkerContext> getWorkerContextClass() {
    return workerContextClass;
  }

  /**
   * Create a user worker context
   *
   * @param graphState State of the graph from the worker
   * @return Instantiated user worker context
   */
  @SuppressWarnings("rawtypes")
  public WorkerContext createWorkerContext(GraphState<I, V, E, M> graphState) {
    WorkerContext workerContext =
        ReflectionUtils.newInstance(workerContextClass, this);
    workerContext.setGraphState(graphState);
    return workerContext;
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.graph.MasterCompute}
   *
   * @return User's master class
   */
  public Class<? extends MasterCompute> getMasterComputeClass() {
    return masterComputeClass;
  }

  /**
   * Create a user master
   *
   * @return Instantiated user master
   */
  public MasterCompute createMasterCompute() {
    return ReflectionUtils.newInstance(masterComputeClass, this);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.graph.Vertex}
   *
   * @return User's vertex class
   */
  public Class<? extends Vertex<I, V, E, M>> getVertexClass() {
    return vertexClass;
  }

  /**
   * Create a user vertex
   *
   * @return Instantiated user vertex
   */
  public Vertex<I, V, E, M> createVertex() {
    return ReflectionUtils.newInstance(vertexClass, this);
  }

  /**
   * Get the user's subclassed vertex index class.
   *
   * @return User's vertex index class
   */
  public Class<I> getVertexIdClass() {
    return vertexIdClass;
  }

  /**
   * Create a user vertex index
   *
   * @return Instantiated user vertex index
   */
  public I createVertexId() {
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
   * @return User's vertex value class
   */
  public Class<V> getVertexValueClass() {
    return vertexValueClass;
  }

  /**
   * Create a user vertex value
   *
   * @return Instantiated user vertex value
   */
  @SuppressWarnings("unchecked")
  public V createVertexValue() {
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
   * @return User's vertex edge value class
   */
  public Class<E> getEdgeValueClass() {
    return edgeValueClass;
  }

  /**
   * Create a user edge value
   *
   * @return Instantiated user edge value
   */
  public E createEdgeValue() {
    if (edgeValueClass == NullWritable.class) {
      return (E) NullWritable.get();
    } else {
      try {
        return edgeValueClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(
            "createEdgeValue: Failed to instantiate", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(
            "createEdgeValue: Illegally accessed", e);
      }
    }
  }

  /**
   * Get the user's subclassed vertex message value class.
   *
   * @return User's vertex message value class
   */
  @SuppressWarnings("unchecked")
  public Class<M> getMessageValueClass() {
    return messageValueClass;
  }

  /**
   * Create a user vertex message value
   *
   * @return Instantiated user vertex message value
   */
  public M createMessageValue() {
    if (messageValueClass == NullWritable.class) {
      return (M) NullWritable.get();
    } else {
      try {
        return messageValueClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(
            "createMessageValue: Failed to instantiate", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(
            "createMessageValue: Illegally accessed", e);
      }
    }
  }

  /**
   * Create a partition
   *
   * @param id Partition id
   * @param progressable Progressable for reporting progress
   * @return Instantiated partition
   */
  public Partition<I, V, E, M> createPartition(
      int id, Progressable progressable) {
    Partition<I, V, E, M> partition =
        ReflectionUtils.newInstance(partitionClass, this);
    partition.initialize(id, progressable);
    return partition;
  }

  /**
   * Use unsafe serialization?
   *
   * @return True if using unsafe serialization, false otherwise.
   */
  public boolean useUnsafeSerialization() {
    return useUnsafeSerialization;
  }

  /**
   * Create an extended data output (can be subclassed)
   *
   * @return ExtendedDataOutput object
   */
  public ExtendedDataOutput createExtendedDataOutput() {
    if (useUnsafeSerialization) {
      return new UnsafeByteArrayOutputStream();
    } else {
      return new ExtendedByteArrayDataOutput();
    }
  }

  /**
   * Create an extended data output (can be subclassed)
   *
   * @param expectedSize Expected size
   * @return ExtendedDataOutput object
   */
  public ExtendedDataOutput createExtendedDataOutput(int expectedSize) {
    if (useUnsafeSerialization) {
      return new UnsafeByteArrayOutputStream(expectedSize);
    } else {
      return new ExtendedByteArrayDataOutput(expectedSize);
    }
  }

  /**
   * Create an extended data output (can be subclassed)
   *
   * @param buf Buffer to use for the output (reuse perhaps)
   * @param pos How much of the buffer is already used
   * @return ExtendedDataOutput object
   */
  public ExtendedDataOutput createExtendedDataOutput(byte[] buf,
                                                     int pos) {
    if (useUnsafeSerialization) {
      return new UnsafeByteArrayOutputStream(buf, pos);
    } else {
      return new ExtendedByteArrayDataOutput(buf, pos);
    }
  }

  /**
   * Create an extended data input (can be subclassed)
   *
   * @param buf Buffer to use for the input
   * @param off Where to start reading in the buffer
   * @param length Maximum length of the buffer
   * @return ExtendedDataInput object
   */
  public ExtendedDataInput createExtendedDataInput(
      byte[] buf, int off, int length) {
    if (useUnsafeSerialization) {
      return new UnsafeByteArrayInputStream(buf, off, length);
    } else {
      return new ExtendedByteArrayDataInput(buf, off, length);
    }
  }
}
