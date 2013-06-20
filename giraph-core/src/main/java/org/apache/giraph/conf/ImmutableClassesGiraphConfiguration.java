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
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.ComputationFactory;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.io.internal.WrappedEdgeInputFormat;
import org.apache.giraph.io.internal.WrappedVertexInputFormat;
import org.apache.giraph.io.internal.WrappedVertexOutputFormat;
import org.apache.giraph.io.superstep_output.MultiThreadedSuperstepOutput;
import org.apache.giraph.io.superstep_output.NoOpSuperstepOutput;
import org.apache.giraph.io.superstep_output.SuperstepOutput;
import org.apache.giraph.io.superstep_output.SynchronizedSuperstepOutput;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Progressable;

/**
 * The classes set here are immutable, the remaining configuration is mutable.
 * Classes are immutable and final to provide the best performance for
 * instantiation.  Everything is thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("unchecked")
public class ImmutableClassesGiraphConfiguration<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends GiraphConfiguration {
  /** Holder for all the classes */
  private final GiraphClasses classes;
  /** Vertex value factory. */
  private final VertexValueFactory<V> vertexValueFactory;

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
    classes = new GiraphClasses<I, V, E>(conf);
    useUnsafeSerialization = USE_UNSAFE_SERIALIZATION.get(this);
    try {
      vertexValueFactory = (VertexValueFactory<V>)
          classes.getVertexValueFactoryClass().newInstance();
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(
          "ImmutableClassesGiraphConfiguration: Failed to instantiate class " +
              classes.getVertexValueFactoryClass(), e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(
          "ImmutableClassesGiraphConfiguration: Illegally accessed class " +
              classes.getVertexValueFactoryClass(), e);
    }
    vertexValueFactory.initialize(this);
  }

  /**
   * Configure an object with this instance if the object is configurable.
   *
   * @param obj Object
   */
  public void configureIfPossible(Object obj) {
    if (obj instanceof ImmutableClassesGiraphConfigurable) {
      ((ImmutableClassesGiraphConfigurable) obj).setConf(this);
    }
  }

  /**
   * Get the vertex input filter class
   *
   * @return VertexInputFilter class
   */
  public Class<? extends EdgeInputFilter<I, E>>
  getEdgeInputFilterClass() {
    return classes.getEdgeInputFilterClass();
  }

  /**
   * Get the edge input filter to use
   *
   * @return EdgeInputFilter
   */
  public EdgeInputFilter getEdgeInputFilter() {
    return ReflectionUtils.newInstance(getEdgeInputFilterClass(), this);
  }

  /**
   * Get the vertex input filter class
   *
   * @return VertexInputFilter class
   */
  public Class<? extends VertexInputFilter<I, V, E>>
  getVertexInputFilterClass() {
    return classes.getVertexInputFilterClass();
  }

  /**
   * Get the vertex input filter to use
   *
   * @return VertexInputFilter
   */
  public VertexInputFilter getVertexInputFilter() {
    return ReflectionUtils.newInstance(getVertexInputFilterClass(), this);
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.partition.GraphPartitionerFactory}.
   *
   * @return User's graph partitioner
   */
  public Class<? extends GraphPartitionerFactory<I, V, E>>
  getGraphPartitionerClass() {
    return classes.getGraphPartitionerFactoryClass();
  }

  /**
   * Create a user graph partitioner class
   *
   * @return Instantiated user graph partitioner class
   */
  public GraphPartitionerFactory<I, V, E> createGraphPartitioner() {
    Class<? extends GraphPartitionerFactory<I, V, E>> klass =
        classes.getGraphPartitionerFactoryClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  @Override
  public boolean hasVertexInputFormat() {
    return classes.hasVertexInputFormat();
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.io.VertexInputFormat}.
   *
   * @return User's vertex input format class
   */
  public Class<? extends VertexInputFormat<I, V, E>>
  getVertexInputFormatClass() {
    return classes.getVertexInputFormatClass();
  }

  /**
   * Create a user vertex input format class.
   * Note: Giraph should only use WrappedVertexInputFormat,
   * which makes sure that Configuration parameters are set properly.
   *
   * @return Instantiated user vertex input format class
   */
  private VertexInputFormat<I, V, E> createVertexInputFormat() {
    Class<? extends VertexInputFormat<I, V, E>> klass =
        getVertexInputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Create a wrapper for user vertex input format,
   * which makes sure that Configuration parameters are set properly in all
   * methods related to this format.
   *
   * @return Wrapper around user vertex input format
   */
  public WrappedVertexInputFormat<I, V, E> createWrappedVertexInputFormat() {
    WrappedVertexInputFormat<I, V, E> wrappedVertexInputFormat =
        new WrappedVertexInputFormat<I, V, E>(createVertexInputFormat());
    configureIfPossible(wrappedVertexInputFormat);
    return wrappedVertexInputFormat;
  }

  @Override
  public void setVertexInputFormatClass(
      Class<? extends VertexInputFormat> vertexInputFormatClass) {
    super.setVertexInputFormatClass(vertexInputFormatClass);
    classes.setVertexInputFormatClass(vertexInputFormatClass);
  }

  @Override
  public boolean hasVertexOutputFormat() {
    return classes.hasVertexOutputFormat();
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
   * Create a user vertex output format class.
   * Note: Giraph should only use WrappedVertexOutputFormat,
   * which makes sure that Configuration parameters are set properly.
   *
   * @return Instantiated user vertex output format class
   */
  private VertexOutputFormat<I, V, E> createVertexOutputFormat() {
    Class<? extends VertexOutputFormat<I, V, E>> klass =
        getVertexOutputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Create a wrapper for user vertex output format,
   * which makes sure that Configuration parameters are set properly in all
   * methods related to this format.
   *
   * @return Wrapper around user vertex output format
   */
  public WrappedVertexOutputFormat<I, V, E> createWrappedVertexOutputFormat() {
    WrappedVertexOutputFormat<I, V, E> wrappedVertexOutputFormat =
        new WrappedVertexOutputFormat<I, V, E>(createVertexOutputFormat());
    configureIfPossible(wrappedVertexOutputFormat);
    return wrappedVertexOutputFormat;
  }

  /**
   * Create the proper superstep output, based on the configuration settings.
   *
   * @param context Mapper context
   * @return SuperstepOutput
   */
  public SuperstepOutput<I, V, E> createSuperstepOutput(
      Mapper<?, ?, ?, ?>.Context context) {
    if (doOutputDuringComputation()) {
      if (vertexOutputFormatThreadSafe()) {
        return new MultiThreadedSuperstepOutput<I, V, E>(this, context);
      } else {
        return new SynchronizedSuperstepOutput<I, V, E>(this, context);
      }
    } else {
      return new NoOpSuperstepOutput<I, V, E>();
    }
  }

  @Override
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
   * Create a user edge input format class.
   * Note: Giraph should only use WrappedEdgeInputFormat,
   * which makes sure that Configuration parameters are set properly.
   *
   * @return Instantiated user edge input format class
   */
  private EdgeInputFormat<I, E> createEdgeInputFormat() {
    Class<? extends EdgeInputFormat<I, E>> klass = getEdgeInputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Create a wrapper for user edge input format,
   * which makes sure that Configuration parameters are set properly in all
   * methods related to this format.
   *
   * @return Wrapper around user edge input format
   */
  public WrappedEdgeInputFormat<I, E> createWrappedEdgeInputFormat() {
    WrappedEdgeInputFormat<I, E> wrappedEdgeInputFormat =
        new WrappedEdgeInputFormat<I, E>(createEdgeInputFormat());
    configureIfPossible(wrappedEdgeInputFormat);
    return wrappedEdgeInputFormat;
  }

  @Override
  public void setEdgeInputFormatClass(
      Class<? extends EdgeInputFormat> edgeInputFormatClass) {
    super.setEdgeInputFormatClass(edgeInputFormatClass);
    classes.setEdgeInputFormatClass(edgeInputFormatClass);
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
   * Get the user's subclassed {@link Combiner} class.
   *
   * @return User's combiner class
   */
  public Class<? extends Combiner<I, ? extends Writable>> getCombinerClass() {
    return classes.getCombinerClass();
  }

  /**
   * Create a user combiner class
   *
   * @param <M> Message data
   * @return Instantiated user combiner class
   */
  @SuppressWarnings("rawtypes")
  public <M extends Writable> Combiner<I, M> createCombiner() {
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
  public Class<? extends VertexResolver<I, V, E>> getVertexResolverClass() {
    return classes.getVertexResolverClass();
  }

  /**
   * Create a user vertex revolver
   *
   * @return Instantiated user vertex resolver
   */
  public VertexResolver<I, V, E> createVertexResolver() {
    return ReflectionUtils.newInstance(getVertexResolverClass(), this);
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
   * @return Instantiated user worker context
   */
  public WorkerContext createWorkerContext() {
    return ReflectionUtils.newInstance(getWorkerContextClass(), this);
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

  @Override
  public Class<? extends
      Computation<I, V, E, ? extends Writable, ? extends Writable>>
  getComputationClass() {
    return classes.getComputationClass();
  }

  /**
   * Get computation factory class
   *
   * @return computation factory class
   */
  @Override
  public Class<? extends ComputationFactory<I, V, E,
      ? extends Writable, ? extends Writable>>
  getComputationFactoryClass() {
    return classes.getComputationFactoryClass();
  }

  /**
   * Get computation factory
   *
   * @return computation factory
   */
  public ComputationFactory<I, V, E, ? extends Writable, ? extends Writable>
  createComputationFactory() {
    return ReflectionUtils.newInstance(getComputationFactoryClass(), this);
  }

  /**
   * Create a user computation
   *
   * @return Instantiated user computation
   */
  public Computation<I, V, E, ? extends Writable, ? extends Writable>
  createComputation() {
    return createComputationFactory().getComputation(this);
  }

  /**
   * Get user types describing graph (I,V,E,M1,M2)
   *
   * @return GiraphTypes
   */
  public GiraphTypes<I, V, E> getGiraphTypes() {
    return classes.getGiraphTypes();
  }

  /**
   * Create a vertex
   *
   * @return Instantiated vertex
   */
  public Vertex<I, V, E> createVertex() {
    Vertex<I, V, E> vertex = new DefaultVertex<I, V, E>();
    vertex.setConf(this);
    return vertex;
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
    return vertexValueFactory.createVertexValue();
  }

  /**
   * Get the user's subclassed vertex value factory class
   *
   * @return User's vertex value factory class
   */
  public Class<? extends VertexValueFactory<V>> getVertexValueFactoryClass() {
    return classes.getVertexValueFactoryClass();
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
   *
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
      return (Edge<I, E>) EdgeFactory.create(createVertexId());
    } else {
      return EdgeFactory.create(createVertexId(), createEdgeValue());
    }
  }

  /**
   * Create a reusable edge.
   *
   * @return Instantiated reusable edge.
   */
  public ReusableEdge<I, E> createReusableEdge() {
    if (isEdgeValueNullWritable()) {
      return (ReusableEdge<I, E>) EdgeFactory.createReusable(createVertexId());
    } else {
      return EdgeFactory.createReusable(createVertexId(), createEdgeValue());
    }
  }

  /**
   * Get the user's subclassed incoming message value class.
   *
   * @param <M> Message data
   * @return User's vertex message value class
   */
  public <M extends Writable> Class<M> getIncomingMessageValueClass() {
    return classes.getIncomingMessageValueClass();
  }

  /**
   * Get the user's subclassed outgoing message value class.
   *
   * @param <M> Message data
   * @return User's vertex message value class
   */
  public <M extends Writable> Class<M> getOutgoingMessageValueClass() {
    return classes.getOutgoingMessageValueClass();
  }

  /**
   * Create incoming message value
   *
   * @param <M> Message data
   * @return Incoming message value
   */
  public <M extends Writable> M createIncomingMessageValue() {
    return this.<M>createMessageValue(this.<M>getIncomingMessageValueClass());
  }

  /**
   * Create outgoing message value
   *
   * @param <M> Message data
   * @return Outgoing message value
   */
  public <M extends Writable> M createOutgoingMessageValue() {
    return this.<M>createMessageValue(this.<M>getOutgoingMessageValueClass());
  }

  /**
   * Create a message value
   *
   * @param <M> Message data
   * @param messageClass Message class
   * @return Instantiated message value
   */
  private <M extends Writable> M createMessageValue(
      Class<? extends Writable> messageClass) {
    if (messageClass == NullWritable.class) {
      return (M) NullWritable.get();
    } else {
      return (M) ReflectionUtils.newInstance(messageClass);
    }
  }

  @Override
  public Class<? extends OutEdges<I, E>> getOutEdgesClass() {
    return classes.getOutEdgesClass();
  }

  /**
   * Get the user's subclassed {@link org.apache.giraph.edge.OutEdges} used for
   * input
   *
   * @return User's input vertex edges class
   */
  public Class<? extends OutEdges<I, E>> getInputOutEdgesClass() {
    return classes.getInputOutEdgesClass();
  }

  /**
   * Check whether the user has specified a different
   * {@link org.apache.giraph.edge.OutEdges} class to be used during
   * edge-based input.
   *
   * @return True iff there is a special edges class for input
   */
  public boolean useInputOutEdges() {
    return classes.getInputOutEdgesClass() != classes.getOutEdgesClass();
  }

  /**
   * Create a user {@link org.apache.giraph.edge.OutEdges}
   *
   * @return Instantiated user OutEdges
   */
  public OutEdges<I, E> createOutEdges() {
    return ReflectionUtils.newInstance(getOutEdgesClass(), this);
  }

  /**
   * Create a {@link org.apache.giraph.edge.OutEdges} instance and initialize
   * it with the default capacity.
   *
   * @return Instantiated OutEdges
   */
  public OutEdges<I, E> createAndInitializeOutEdges() {
    OutEdges<I, E> outEdges = createOutEdges();
    outEdges.initialize();
    return outEdges;
  }

  /**
   * Create a {@link org.apache.giraph.edge.OutEdges} instance and initialize
   * it with the given capacity (the number of edges that will be added).
   *
   * @param capacity Number of edges that will be added
   * @return Instantiated OutEdges
   */
  public OutEdges<I, E> createAndInitializeOutEdges(int capacity) {
    OutEdges<I, E> outEdges = createOutEdges();
    outEdges.initialize(capacity);
    return outEdges;
  }

  /**
   * Create a {@link org.apache.giraph.edge.OutEdges} instance and initialize
   * it with the given iterable of edges.
   *
   * @param edges Iterable of edges to add
   * @return Instantiated OutEdges
   */
  public OutEdges<I, E> createAndInitializeOutEdges(
      Iterable<Edge<I, E>> edges) {
    OutEdges<I, E> outEdges = createOutEdges();
    outEdges.initialize(edges);
    return outEdges;
  }

  /**
   * Create a user {@link org.apache.giraph.edge.OutEdges} used during
   * edge-based input
   *
   * @return Instantiated user input OutEdges
   */
  public OutEdges<I, E> createInputOutEdges() {
    return ReflectionUtils.newInstance(getInputOutEdgesClass(), this);
  }

  /**
   * Create an input {@link org.apache.giraph.edge.OutEdges} instance and
   * initialize it with the default capacity.
   *
   * @return Instantiated input OutEdges
   */
  public OutEdges<I, E> createAndInitializeInputOutEdges() {
    OutEdges<I, E> outEdges = createInputOutEdges();
    outEdges.initialize();
    return outEdges;
  }

  /**
   * Create a partition
   *
   * @param id Partition id
   * @param progressable Progressable for reporting progress
   * @return Instantiated partition
   */
  public Partition<I, V, E> createPartition(
      int id, Progressable progressable) {
    Class<? extends Partition<I, V, E>> klass = classes.getPartitionClass();
    Partition<I, V, E> partition = ReflectionUtils.newInstance(klass, this);
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

  /**
   * Update Computation and Combiner class used
   *
   * @param superstepClasses SuperstepClasses
   */
  public void updateSuperstepClasses(SuperstepClasses superstepClasses) {
    classes.setComputationClass(superstepClasses.getComputationClass());
    classes.setCombinerClass(superstepClasses.getCombinerClass());
  }
}
