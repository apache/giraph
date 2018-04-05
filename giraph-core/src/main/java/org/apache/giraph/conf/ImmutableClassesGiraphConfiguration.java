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

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.compression.SnappyFramedDecoder;
import io.netty.handler.codec.compression.SnappyFramedEncoder;

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.factories.ComputationFactory;
import org.apache.giraph.factories.EdgeValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.factories.OutEdgesFactory;
import org.apache.giraph.factories.ValueFactories;
import org.apache.giraph.factories.VertexIdFactory;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Language;
import org.apache.giraph.graph.MapperObserver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.io.internal.WrappedEdgeInputFormat;
import org.apache.giraph.io.internal.WrappedEdgeOutputFormat;
import org.apache.giraph.io.internal.WrappedMappingInputFormat;
import org.apache.giraph.io.internal.WrappedVertexInputFormat;
import org.apache.giraph.io.internal.WrappedVertexOutputFormat;
import org.apache.giraph.io.superstep_output.MultiThreadedSuperstepOutput;
import org.apache.giraph.io.superstep_output.NoOpSuperstepOutput;
import org.apache.giraph.io.superstep_output.SuperstepOutput;
import org.apache.giraph.io.superstep_output.SynchronizedSuperstepOutput;
import org.apache.giraph.job.GiraphJobObserver;
import org.apache.giraph.job.GiraphJobRetryChecker;
import org.apache.giraph.mapping.MappingStore;
import org.apache.giraph.mapping.MappingStoreOps;
import org.apache.giraph.mapping.translate.TranslateEdge;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.master.MasterObserver;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.GcObserver;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.utils.io.BigDataInputOutput;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.giraph.utils.io.ExtendedDataInputOutput;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;


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
    V extends Writable, E extends Writable> extends GiraphConfiguration {
  /** Holder for all the classes */
  private final GiraphClasses classes;
  /** Mapping target class */
  private Class<? extends Writable> mappingTargetClass = null;
  /** Value (IVEMM) Factories */
  private final ValueFactories<I, V, E> valueFactories;
  /** Factory to create {@link OutEdges} for computation */
  private final OutEdgesFactory<I, E> outEdgesFactory;
  /** Factory to create {@link OutEdges} for input */
  private final OutEdgesFactory<I, E> inputOutEdgesFactory;
  /** Language values (IVEMM) are implemented in */
  private final PerGraphTypeEnum<Language> valueLanguages;
  /** Whether values (IVEMM) need Jython wrappers */
  private final PerGraphTypeBoolean valueNeedsWrappers;


  /**
   * Use unsafe serialization? Cached for fast access to instantiate the
   * extended data input/output classes
   */
  private final boolean useUnsafeSerialization;
  /**
   * Use BigDataIO for messages? Cached for fast access to instantiate the
   * extended data input/output classes for messages
   */
  private final boolean useBigDataIOForMessages;
  /** Is the graph static (meaning there is no mutation)? */
  private final boolean isStaticGraph;
  /** Whether or not to use message size encoding */
  private final boolean useMessageSizeEncoding;

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
    useBigDataIOForMessages = USE_BIG_DATA_IO_FOR_MESSAGES.get(this);
    valueLanguages = PerGraphTypeEnum.readFromConf(
        GiraphConstants.GRAPH_TYPE_LANGUAGES, conf);
    valueNeedsWrappers = PerGraphTypeBoolean.readFromConf(
        GiraphConstants.GRAPH_TYPES_NEEDS_WRAPPERS, conf);
    isStaticGraph = GiraphConstants.STATIC_GRAPH.get(this);
    valueFactories = new ValueFactories<I, V, E>(this);
    outEdgesFactory = VERTEX_EDGES_FACTORY_CLASS.newInstance(this);
    inputOutEdgesFactory = INPUT_VERTEX_EDGES_FACTORY_CLASS.newInstance(this);
    useMessageSizeEncoding = USE_MESSAGE_SIZE_ENCODING.get(conf);
  }

  /**
   * Configure an object with this instance if the object is configurable.
   *
   * @param obj Object
   */
  public void configureIfPossible(Object obj) {
    if (obj instanceof GiraphConfigurationSettable) {
      ((GiraphConfigurationSettable) obj).setConf(this);
    }
  }

  public PerGraphTypeBoolean getValueNeedsWrappers() {
    return valueNeedsWrappers;
  }

  public PerGraphTypeEnum<Language> getValueLanguages() {
    return valueLanguages;
  }

  /**
   * Get the class used for edge translation during vertex input
   *
   * @return edge translation class
   */
  public Class<? extends TranslateEdge> edgeTranslationClass() {
    return EDGE_TRANSLATION_CLASS.get(this);
  }

  /**
   * Instance of TranslateEdge that contains helper method for edge translation
   *
   * @return instance of TranslateEdge
   */
  public TranslateEdge<I, E> edgeTranslationInstance() {
    if (edgeTranslationClass() != null) {
      return ReflectionUtils.newInstance(edgeTranslationClass(), this);
    }
    return null;
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
   * Get MappingInputFormatClass
   *
   * @return MappingInputFormatClass
   */
  public Class<? extends MappingInputFormat<I, V, E, ? extends Writable>>
  getMappingInputFormatClass() {
    return classes.getMappingInputFormatClass();
  }

  /**
   * Set MappingInputFormatClass
   *
   * @param mappingInputFormatClass Determines how mappings are input
   */
  @Override
  public void setMappingInputFormatClass(
    Class<? extends MappingInputFormat> mappingInputFormatClass) {
    super.setMappingInputFormatClass(mappingInputFormatClass);
    classes.setMappingInputFormatClass(mappingInputFormatClass);
  }

  /**
   * Check if mappingInputFormat is set
   *
   * @return true if mappingInputFormat is set
   */
  public boolean hasMappingInputFormat() {
    return classes.hasMappingInputFormat();
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
   * Create a user mapping input format class.
   * Note: Giraph should only use WrappedMappingInputFormat,
   * which makes sure that Configuration parameters are set properly.
   *
   * @return Instantiated user mapping input format class
   */
  private MappingInputFormat<I, V, E, ? extends Writable>
  createMappingInputFormat() {
    Class<? extends MappingInputFormat<I, V, E, ? extends Writable>> klass =
        getMappingInputFormatClass();
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
   * Create a wrapper for user mapping input format,
   * which makes sure that Configuration parameters are set properly in all
   * methods related to this format.
   *
   * @return Wrapper around user mapping input format
   */
  public WrappedMappingInputFormat<I, V, E, ? extends Writable>
  createWrappedMappingInputFormat() {
    WrappedMappingInputFormat<I, V, E, ? extends Writable>
      wrappedMappingInputFormat =
        new WrappedMappingInputFormat<>(createMappingInputFormat());
    configureIfPossible(wrappedMappingInputFormat);
    return wrappedMappingInputFormat;
  }

  @Override
  public boolean hasEdgeOutputFormat() {
    return classes.hasEdgeOutputFormat();
  }

  /**
   * Get the user's subclassed
   * {@link org.apache.giraph.io.EdgeOutputFormat}.
   *
   * @return User's edge output format class
   */
  public Class<? extends EdgeOutputFormat<I, V, E>>
  getEdgeOutputFormatClass() {
    return classes.getEdgeOutputFormatClass();
  }

  /**
   * Create a user edge output format class.
   * Note: Giraph should only use WrappedEdgeOutputFormat,
   * which makes sure that Configuration parameters are set properly.
   *
   * @return Instantiated user edge output format class
   */
  private EdgeOutputFormat<I, V, E> createEdgeOutputFormat() {
    Class<? extends EdgeOutputFormat<I, V, E>> klass =
        getEdgeOutputFormatClass();
    return ReflectionUtils.newInstance(klass, this);
  }

  /**
   * Create a wrapper for user edge output format,
   * which makes sure that Configuration parameters are set properly in all
   * methods related to this format.
   *
   * @return Wrapper around user edge output format
   */
  public WrappedEdgeOutputFormat<I, V, E> createWrappedEdgeOutputFormat() {
    WrappedEdgeOutputFormat<I, V, E> wrappedEdgeOutputFormat =
        new WrappedEdgeOutputFormat<I, V, E>(createEdgeOutputFormat());
    configureIfPossible(wrappedEdgeOutputFormat);
    return wrappedEdgeOutputFormat;
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
   * Get the user's subclassed
   * {@link org.apache.giraph.graph.VertexValueCombiner} class.
   *
   * @return User's vertex value combiner class
   */
  public Class<? extends VertexValueCombiner<V>>
  getVertexValueCombinerClass() {
    return classes.getVertexValueCombinerClass();
  }

  /**
   * Create a user vertex value combiner class
   *
   * @return Instantiated user vertex value combiner class
   */
  @SuppressWarnings("rawtypes")
  public VertexValueCombiner<V> createVertexValueCombiner() {
    return ReflectionUtils.newInstance(getVertexValueCombinerClass(), this);
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
  public Class<? extends Computation<I, V, E,
      ? extends Writable, ? extends Writable>>
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
    return createComputationFactory().createComputation(this);
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
    Class vertexClass = classes.getVertexClass();
    return (Vertex<I, V, E>) ReflectionUtils.newInstance(vertexClass, this);
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
   * Get vertex ID factory
   *
   * @return {@link VertexIdFactory}
   */
  public VertexIdFactory<I> getVertexIdFactory() {
    return valueFactories.getVertexIdFactory();
  }

  /**
   * Create a user vertex index
   *
   * @return Instantiated user vertex index
   */
  public I createVertexId() {
    return getVertexIdFactory().newInstance();
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
   * Get vertex value factory
   *
   * @return {@link VertexValueFactory}
   */
  public VertexValueFactory<V> getVertexValueFactory() {
    return valueFactories.getVertexValueFactory();
  }

  /**
   * Create a user vertex value
   *
   * @return Instantiated user vertex value
   */
  @SuppressWarnings("unchecked")
  public V createVertexValue() {
    return getVertexValueFactory().newInstance();
  }

  /**
   * Get the user's subclassed vertex value factory class
   *
   * @return User's vertex value factory class
   */
  public Class<? extends VertexValueFactory<V>> getVertexValueFactoryClass() {
    return (Class<? extends VertexValueFactory<V>>)
        valueFactories.getVertexValueFactory().getClass();
  }

  /**
   * Create array of MasterObservers.
   *
   * @param context Mapper context
   * @return Instantiated array of MasterObservers.
   */
  public MasterObserver[] createMasterObservers(
      Mapper<?, ?, ?, ?>.Context context) {
    Class<? extends MasterObserver>[] klasses = getMasterObserverClasses();
    MasterObserver[] objects = new MasterObserver[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      objects[i] = ReflectionUtils.newInstance(klasses[i], this, context);
    }
    return objects;
  }

  /**
   * Create array of WorkerObservers.
   *
   * @param context Mapper context
   * @return Instantiated array of WorkerObservers.
   */
  public WorkerObserver[] createWorkerObservers(
      Mapper<?, ?, ?, ?>.Context context) {
    Class<? extends WorkerObserver>[] klasses = getWorkerObserverClasses();
    WorkerObserver[] objects = new WorkerObserver[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      objects[i] = ReflectionUtils.newInstance(klasses[i], this, context);
    }
    return objects;
  }

  /**
   * Create array of MapperObservers.
   *
   * @param context Mapper context
   * @return Instantiated array of MapperObservers.
   */
  public MapperObserver[] createMapperObservers(
      Mapper<?, ?, ?, ?>.Context context) {
    Class<? extends MapperObserver>[] klasses = getMapperObserverClasses();
    MapperObserver[] objects = new MapperObserver[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      objects[i] = ReflectionUtils.newInstance(klasses[i], this, context);
    }
    return objects;
  }

  /**
   * Create array of GcObservers.
   *
   * @param context Mapper context
   * @return Instantiated array of GcObservers.
   */
  public GcObserver[] createGcObservers(
      Mapper<?, ?, ?, ?>.Context context) {
    Class<? extends GcObserver>[] klasses = getGcObserverClasses();
    GcObserver[] objects = new GcObserver[klasses.length];
    for (int i = 0; i < klasses.length; ++i) {
      objects[i] = ReflectionUtils.newInstance(klasses[i], this, context);
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
   * Create job retry checker
   *
   * @return GiraphJobRetryChecker set in configuration.
   */
  public GiraphJobRetryChecker getJobRetryChecker() {
    return ReflectionUtils.newInstance(getJobRetryCheckerClass(), this);
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
   * Get Factory for creating edge values
   *
   * @return {@link EdgeValueFactory}
   */
  public EdgeValueFactory<E> getEdgeValueFactory() {
    return valueFactories.getEdgeValueFactory();
  }

  /**
   * Create a user edge value
   *
   * @return Instantiated user edge value
   */
  public E createEdgeValue() {
    return getEdgeValueFactory().newInstance();
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
   * Create edge based on #createEdge definition
   *
   * @param translateEdge instance of TranslateEdge
   * @param edge edge to be translated
   * @return translated edge
   */
  public Edge<I, E> createEdge(TranslateEdge<I, E>
    translateEdge, Edge<I, E> edge) {
    I translatedId = translateEdge.translateId(edge.getTargetVertexId());
    if (isEdgeValueNullWritable()) {
      return (Edge<I, E>) EdgeFactory.create(translatedId);
    } else {
      return EdgeFactory.create(translatedId,
        translateEdge.cloneValue(edge.getValue()));
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
   * Create edge store factory
   *
   * @return edge store factory
   */
  public EdgeStoreFactory<I, V, E> createEdgeStoreFactory() {
    Class<? extends EdgeStoreFactory> edgeStoreFactoryClass =
        EDGE_STORE_FACTORY_CLASS.get(this);
    return ReflectionUtils.newInstance(edgeStoreFactoryClass);
  }

  /**
   * Get the user's subclassed incoming message value class.
   *
   * @param <M> Message data
   * @return User's vertex message value class
   */
  public <M extends Writable> Class<M> getIncomingMessageValueClass() {
    return classes.getIncomingMessageClasses().getMessageClass();
  }

  /**
   * Get the user's subclassed outgoing message value class.
   *
   * @param <M> Message type
   * @return User's vertex message value class
   */
  public <M extends Writable> Class<M> getOutgoingMessageValueClass() {
    return classes.getOutgoingMessageClasses().getMessageClass();
  }

  /**
   * Get incoming message classes
   * @param <M> message type
   * @return incoming message classes
   */
  public <M extends Writable>
  MessageClasses<I, M> getIncomingMessageClasses() {
    return classes.getIncomingMessageClasses();
  }

  /**
   * Get outgoing message classes
   * @param <M> message type
   * @return outgoing message classes
   */
  public <M extends Writable>
  MessageClasses<I, M> getOutgoingMessageClasses() {
    return classes.getOutgoingMessageClasses();
  }

  /**
   * Create new outgoing message value factory
   * @param <M> message type
   * @return outgoing message value factory
   */
  public <M extends Writable>
  MessageValueFactory<M> createOutgoingMessageValueFactory() {
    return classes.getOutgoingMessageClasses().createMessageValueFactory(this);
  }

  /**
   * Create new incoming message value factory
   * @param <M> message type
   * @return incoming message value factory
   */
  public <M extends Writable>
  MessageValueFactory<M> createIncomingMessageValueFactory() {
    return classes.getIncomingMessageClasses().createMessageValueFactory(this);
  }

  @Override
  public void setMessageCombinerClass(
      Class<? extends MessageCombiner> messageCombinerClass) {
    throw new IllegalArgumentException(
        "Cannot set message combiner on ImmutableClassesGiraphConfigurable");
  }

  /**
   * Create a user combiner class
   *
   * @param <M> Message data
   * @return Instantiated user combiner class
   */
  public <M extends Writable> MessageCombiner<? super I, M>
  createOutgoingMessageCombiner() {
    return classes.getOutgoingMessageClasses().createMessageCombiner(this);
  }

  /**
   * Check if user set a combiner
   *
   * @return True iff user set a combiner class
   */
  public boolean useOutgoingMessageCombiner() {
    return classes.getOutgoingMessageClasses().useMessageCombiner();
  }

  /**
   * Get outgoing message encode and store type
   * @return outgoing message encode and store type
   */
  public MessageEncodeAndStoreType getOutgoingMessageEncodeAndStoreType() {
    return classes.getOutgoingMessageClasses().getMessageEncodeAndStoreType();
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
   * Get MappingStore class to be used
   *
   * @return MappingStore class set by user
   */
  public Class<? extends MappingStore> getMappingStoreClass() {
    return MAPPING_STORE_CLASS.get(this);
  }

  /**
   * Create a {@link org.apache.giraph.mapping.MappingStore} instance
   *
   * @return MappingStore Instance
   */
  public MappingStore<I, ? extends Writable> createMappingStore() {
    if (getMappingStoreClass() != null) {
      return ReflectionUtils.newInstance(getMappingStoreClass(), this);
    } else {
      return null;
    }
  }

  /**
   * Get MappingStoreOps class to be used
   *
   * @return MappingStoreOps class set by user
   */
  public Class<? extends MappingStoreOps> getMappingStoreOpsClass() {
    return MAPPING_STORE_OPS_CLASS.get(this);
  }

  /**
   * Create a {@link org.apache.giraph.mapping.MappingStoreOps} instance
   *
   * @return MappingStoreOps Instance
   */
  public MappingStoreOps<I, ? extends Writable> createMappingStoreOps() {
    if (getMappingStoreOpsClass() != null) {
      return ReflectionUtils.newInstance(getMappingStoreOpsClass(), this);
    } else {
      return null;
    }
  }

  /**
   * Get mappingTarget class
   *
   * @return mappingTarget class
   */
  public Class<? extends Writable> getMappingTargetClass() {
    if (mappingTargetClass == null) {
      Class<?>[] classList = ReflectionUtils.getTypeArguments(
        MappingStore.class, getMappingStoreClass());
      Preconditions.checkArgument(classList.length == 2);
      mappingTargetClass = (Class<? extends Writable>) classList[1];
    }
    return mappingTargetClass;
  }

  /**
   * Create and return mappingTarget instance
   *
   * @return mappingTarget instance
   */
  public Writable createMappingTarget() {
    return WritableUtils.createWritable(getMappingTargetClass());
  }

  /**
   * Create a user {@link org.apache.giraph.edge.OutEdges}
   *
   * @return Instantiated user OutEdges
   */
  public OutEdges<I, E> createOutEdges() {
    return outEdgesFactory.newInstance();
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
    return inputOutEdgesFactory.newInstance();
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
   * Create DataInputOutput to store messages
   *
   * @return DataInputOutput object
   */
  public DataInputOutput createMessagesInputOutput() {
    if (useBigDataIOForMessages) {
      return new BigDataInputOutput(this);
    } else {
      return new ExtendedDataInputOutput(this);
    }
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
   * Create an extended data input (can be subclassed)
   *
   * @param buf Buffer to use for the input
   * @return ExtendedDataInput object
   */
  public ExtendedDataInput createExtendedDataInput(byte[] buf) {
    if (useUnsafeSerialization) {
      return new UnsafeByteArrayInputStream(buf);
    } else {
      return new ExtendedByteArrayDataInput(buf);
    }
  }

  /**
   * Create extendedDataInput based on extendedDataOutput
   *
   * @param extendedDataOutput extendedDataOutput
   * @return extendedDataInput
   */
  public ExtendedDataInput createExtendedDataInput(
    ExtendedDataOutput extendedDataOutput) {
    return createExtendedDataInput(extendedDataOutput.getByteArray(), 0,
        extendedDataOutput.getPos());
  }

  /**
   * Whether to use an unsafe serialization
   *
   * @return whether to use unsafe serialization
   */
  public boolean getUseUnsafeSerialization() {
    return useUnsafeSerialization;
  }

  /**
   * Update Computation and MessageCombiner class used
   *
   * @param superstepClasses SuperstepClasses
   */
  public void updateSuperstepClasses(SuperstepClasses superstepClasses) {
    superstepClasses.updateGiraphClasses(classes);
  }

  /**
   * Has the user enabled compression in netty client &amp; server
   *
   * @return true if ok to do compression of netty requests
   */
  public boolean doCompression() {
    switch (GiraphConstants.NETTY_COMPRESSION_ALGORITHM.get(this)) {
    case "SNAPPY":
      return true;
    case "INFLATE":
      return true;
    default:
      return false;
    }
  }

  /**
   * Get encoder for message compression in netty
   *
   * @return message to byte encoder
   */
  public MessageToByteEncoder getNettyCompressionEncoder() {
    switch (GiraphConstants.NETTY_COMPRESSION_ALGORITHM.get(this)) {
    case "SNAPPY":
      return new SnappyFramedEncoder();
    case "INFLATE":
      return new JdkZlibEncoder();
    default:
      return null;
    }
  }

  /**
   * Get decoder for message decompression in netty
   *
   * @return byte to message decoder
   */
  public ByteToMessageDecoder getNettyCompressionDecoder() {
    switch (GiraphConstants.NETTY_COMPRESSION_ALGORITHM.get(this)) {
    case "SNAPPY":
      return new SnappyFramedDecoder(true);
    case "INFLATE":
      return new JdkZlibDecoder();
    default:
      return null;
    }
  }

  /**
   * Whether the application with change or not the graph topology.
   *
   * @return true if the graph is static, false otherwise.
   */
  public boolean isStaticGraph() {
    return isStaticGraph;
  }

  /**
   * @return job id
   */
  public String getJobId() {
    return get("mapred.job.id", "UnknownJob");
  }

  /**
   * Use message size encoding?  This feature may help with complex message
   * objects.
   *
   * @return Whether to use message size encoding
   */
  public boolean useMessageSizeEncoding() {
    return useMessageSizeEncoding;
  }
}
