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

package org.apache.giraph.conf;

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeNoValue;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.MutableEdge;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.vertex.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
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
    V extends Writable, E extends Writable, M extends Writable>
    extends GiraphConfiguration {
  /** Master graph partitioner - cached for fast access */
  protected final MasterGraphPartitioner<I, V, E, M> masterGraphPartitioner;

  /** Holder for all the classes */
  private final GiraphClasses classes;

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

    classes = new GiraphClasses(conf);
    masterGraphPartitioner = (MasterGraphPartitioner<I, V, E, M>)
        createGraphPartitioner().createMasterGraphPartitioner();

    useUnsafeSerialization = getBoolean(USE_UNSAFE_SERIALIZATION,
        USE_UNSAFE_SERIALIZATION_DEFAULT);
  }

  /**
   * Configure an object with this instance if the object is configurable.
   * @param obj Object
   */
  public void configureIfPossible(Object obj) {
    if (obj instanceof ImmutableClassesGiraphConfigurable) {
      ((ImmutableClassesGiraphConfigurable) obj).setConf(this);
    }
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.partition.GraphPartitionerFactory}.
   *
   * @return User's graph partitioner
   */
  public Class<? extends GraphPartitionerFactory<I, V, E, M>>
  getGraphPartitionerClass() {
    return classes.getGraphPartitionerFactoryClass();
  }

  /**
   * Create a user graph partitioner class
   *
   * @return Instantiated user graph partitioner class
   */
  public GraphPartitionerFactory<I, V, E, M> createGraphPartitioner() {
    Class<? extends GraphPartitionerFactory<I, V, E, M>> klass =
        classes.getGraphPartitionerFactoryClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Create a user graph partitioner partition stats class
   *
   * @return Instantiated user graph partition stats class
   */
  public PartitionStats createGraphPartitionStats() {
    return getMasterGraphPartitioner().createPartitionStats();
  }

  /**
   * Get the cached MasterGraphPartitioner.
   *
   * @return MasterGraphPartitioner cached in this class.
   */
  public MasterGraphPartitioner<I, V, E, M> getMasterGraphPartitioner() {
    return masterGraphPartitioner;
  }

  /**
   * Does the job have a {@link VertexInputFormat}?
   *
   * @return True iff a {@link VertexInputFormat} has been specified.
   */
  public boolean hasVertexInputFormat() {
    return classes.hasVertexInputFormat();
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.io.VertexInputFormat}.
   *
   * @return User's vertex input format class
   */
  public Class<? extends VertexInputFormat<I, V, E, M>>
  getVertexInputFormatClass() {
    return classes.getVertexInputFormatClass();
  }

  /**
   * Create a user vertex input format class
   *
   * @return Instantiated user vertex input format class
   */
  public VertexInputFormat<I, V, E, M>
  createVertexInputFormat() {
    Class<? extends VertexInputFormat<I, V, E, M>> klass =
        classes.getVertexInputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.io.VertexOutputFormat}.
   *
   * @return User's vertex output format class
   */
  public Class<? extends VertexOutputFormat<I, V, E>>
  getVertexOutputFormatClass() {
    return classes.getVertexOutputFormatClass();
  }

  /**
   * Create a user vertex output format class
   *
   * @return Instantiated user vertex output format class
   */
  @SuppressWarnings("rawtypes")
  public VertexOutputFormat<I, V, E> createVertexOutputFormat() {
    Class<? extends VertexOutputFormat<I, V, E>> klass =
        classes.getVertexOutputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Does the job have an {@link EdgeInputFormat}?
   *
   * @return True iff an {@link EdgeInputFormat} has been specified.
   */
  public boolean hasEdgeInputFormat() {
    return classes.hasEdgeInputFormat();
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.io.EdgeInputFormat}.
   *
   * @return User's edge input format class
   */
  public Class<? extends EdgeInputFormat<I, E>> getEdgeInputFormatClass() {
    return classes.getEdgeInputFormatClass();
  }

  /**
   * Create a user edge input format class
   *
   * @return Instantiated user edge input format class
   */
  public EdgeInputFormat<I, E> createEdgeInputFormat() {
    Class<? extends EdgeInputFormat<I, E>> klass = getEdgeInputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Get the user's subclassed {@link AggregatorWriter}.
   *
   * @return User's aggregator writer class
   */
  public Class<? extends AggregatorWriter> getAggregatorWriterClass() {
    return classes.getAggregatorWriterClass();
  }

  /**
   * Create a user aggregator output format class
   *
   * @return Instantiated user aggregator writer class
   */
  public AggregatorWriter createAggregatorWriter() {
    return ReflectionUtils.newInstance(getAggregatorWriterClass(), this);
  }

  /**
   * Create a user combiner class
   *
   * @return Instantiated user combiner class
   */
  @SuppressWarnings("rawtypes")
  public Combiner<I, M> createCombiner() {
    Class<? extends Combiner<I, M>> klass = classes.getCombinerClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Check if user set a combiner
   *
   * @return True iff user set a combiner class
   */
  public boolean useCombiner() {
    return classes.hasCombinerClass();
  }

  /**
   * Get the user's subclassed VertexResolver.
   *
   * @return User's vertex resolver class
   */
  public Class<? extends VertexResolver<I, V, E, M>> getVertexResolverClass() {
    return classes.getVertexResolverClass();
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
        ReflectionUtils.newInstance(getVertexResolverClass(), this);
    resolver.setGraphState(graphState);
    return resolver;
  }

  /**
   * Get the user's subclassed PartitionContext.
   *
   * @return User's partition context class
   */
  public Class<? extends PartitionContext> getPartitionContextClass() {
    return classes.getPartitionContextClass();
  }

  /**
   * Create a user partition context
   *
   * @return Instantiated user partition context
   */
  public PartitionContext createPartitionContext() {
    return ReflectionUtils.newInstance(getPartitionContextClass(), this);
  }

  /**
   * Get the user's subclassed WorkerContext.
   *
   * @return User's worker context class
   */
  public Class<? extends WorkerContext> getWorkerContextClass() {
    return classes.getWorkerContextClass();
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
        ReflectionUtils.newInstance(getWorkerContextClass(), this);
    workerContext.setGraphState(graphState);
    return workerContext;
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.master.MasterCompute}
   *
   * @return User's master class
   */
  public Class<? extends MasterCompute> getMasterComputeClass() {
    return classes.getMasterComputeClass();
  }

  /**
   * Create a user master
   *
   * @return Instantiated user master
   */
  public MasterCompute createMasterCompute() {
    return ReflectionUtils.newInstance(getMasterComputeClass(), this);
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.vertex.Vertex}
   *
   * @return User's vertex class
   */
  public Class<? extends Vertex<I, V, E, M>> getVertexClass() {
    return classes.getVertexClass();
  }

  /**
   * Create a user vertex
   *
   * @return Instantiated user vertex
   */
  public Vertex<I, V, E, M> createVertex() {
    return ReflectionUtils.newInstance(getVertexClass(), this);
  }

  /**
   * Get the user's subclassed vertex index class.
   *
   * @return User's vertex index class
   */
  public Class<I> getVertexIdClass() {
    return classes.getVertexIdClass();
  }

  /**
   * Create a user vertex index
   *
   * @return Instantiated user vertex index
   */
  public I createVertexId() {
    try {
      return getVertexIdClass().newInstance();
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
    return classes.getVertexValueClass();
  }

  /**
   * Create a user vertex value
   *
   * @return Instantiated user vertex value
   */
  @SuppressWarnings("unchecked")
  public V createVertexValue() {
    Class<V> klass = getVertexValueClass();
    if (klass == NullWritable.class) {
      return (V) NullWritable.get();
    } else {
      try {
        return klass.newInstance();
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
   * Create array of MasterObservers.
   *
   * @return Instantiated array of MasterObservers.
   */
  public MasterObserver[] createMasterObservers() {
    Class<? extends MasterObserver>[] klasses = getMasterObserverClasses();
    MasterObserver[] objects = new MasterObserver[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      objects[i] = ReflectionUtils.newInstance(klasses[i], this);
    }
    return objects;
  }

  /**
   * Create array of WorkerObservers.
   *
   * @return Instantiated array of WorkerObservers.
   */
  public WorkerObserver[] createWorkerObservers() {
    Class<? extends WorkerObserver>[] klasses = getWorkerObserverClasses();
    WorkerObserver[] objects = new WorkerObserver[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      objects[i] = ReflectionUtils.newInstance(klasses[i], this);
    }
    return objects;
  }

  /**
   * Create job observer
   * @return GiraphJobObserver set in configuration.
   */
  public GiraphJobObserver getJobObserver() {
    return ReflectionUtils.newInstance(getJobObserverClass(), this);
  }

  /**
   * Get the user's subclassed edge value class.
   *
   * @return User's vertex edge value class
   */
  public Class<E> getEdgeValueClass() {
    return classes.getEdgeValueClass();
  }

  /**
   * Tell if we are using NullWritable for Edge value.
   *
   * @return true if NullWritable is class for
   */
  public boolean isEdgeValueNullWritable() {
    return getEdgeValueClass() == NullWritable.class;
  }

  /**
   * Create a user edge value
   *
   * @return Instantiated user edge value
   */
  public E createEdgeValue() {
    if (isEdgeValueNullWritable()) {
      return (E) NullWritable.get();
    } else {
      Class<E> klass = getEdgeValueClass();
      try {
        return klass.newInstance();
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
   * Create a user edge.
   *
   * @return Instantiated user edge.
   */
  public Edge<I, E> createEdge() {
    if (isEdgeValueNullWritable()) {
      return (Edge<I, E>) new EdgeNoValue<I>(createVertexId());
    } else {
      return new DefaultEdge<I, E>(createVertexId(), createEdgeValue());
    }
  }

  /**
   * Create a mutable user edge.
   *
   * @return Instantiated mutable user edge.
   */
  public MutableEdge<I, E> createMutableEdge() {
    if (isEdgeValueNullWritable()) {
      return (MutableEdge<I, E>) new EdgeNoValue<I>(createVertexId());
    } else {
      return new DefaultEdge<I, E>(createVertexId(), createEdgeValue());
    }
  }

  /**
   * Get the user's subclassed vertex message value class.
   *
   * @return User's vertex message value class
   */
  @SuppressWarnings("unchecked")
  public Class<M> getMessageValueClass() {
    return classes.getMessageValueClass();
  }

  /**
   * Create a user vertex message value
   *
   * @return Instantiated user vertex message value
   */
  public M createMessageValue() {
    Class<M> klass = getMessageValueClass();
    if (klass == NullWritable.class) {
      return (M) NullWritable.get();
    } else {
      try {
        return klass.newInstance();
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
    Class<? extends Partition<I, V, E, M>> klass = classes.getPartitionClass();
    Partition<I, V, E, M> partition = ReflectionUtils.newInstance(klass, this);
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
