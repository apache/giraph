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
import org.apache.giraph.aggregators.TextAggregatorWriter;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.DefaultVertexValueFactory;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.filters.DefaultEdgeInputFilter;
import org.apache.giraph.io.filters.DefaultVertexInputFilter;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.HashPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.SimplePartition;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.List;

/**
 * Holder for classes used by Giraph.
 *
 * @param <I> Vertex ID class
 * @param <V> Vertex Value class
 * @param <E> Edge class
 */
@SuppressWarnings("unchecked")
public class GiraphClasses<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements GiraphConstants {
  /** Computation class - cached for fast access */
  protected Class<? extends
      Computation<I, V, E, ? extends Writable, ? extends Writable>>
  computationClass;
  /** Vertex id class - cached for fast access */
  protected Class<I> vertexIdClass;
  /** Vertex value class - cached for fast access */
  protected Class<V> vertexValueClass;
  /** Edge value class - cached for fast access */
  protected Class<E> edgeValueClass;
  /** Incoming message value class - cached for fast access */
  protected Class<? extends Writable> incomingMessageValueClass;
  /** Outgoing message value class - cached for fast access */
  protected Class<? extends Writable> outgoingMessageValueClass;
  /** Vertex edges class - cached for fast access */
  protected Class<? extends OutEdges<I, E>> outEdgesClass;
  /** Input vertex edges class - cached for fast access */
  protected Class<? extends OutEdges<I, E>> inputOutEdgesClass;

  /** Vertex value factory class - cached for fast access */
  protected Class<? extends VertexValueFactory<V>> vertexValueFactoryClass;

  /** Graph partitioner factory class - cached for fast access */
  protected Class<? extends GraphPartitionerFactory<I, V, E>>
  graphPartitionerFactoryClass;

  /** Vertex input format class - cached for fast access */
  protected Class<? extends VertexInputFormat<I, V, E>>
  vertexInputFormatClass;
  /** Vertex output format class - cached for fast access */
  protected Class<? extends VertexOutputFormat<I, V, E>>
  vertexOutputFormatClass;
  /** Edge input format class - cached for fast access */
  protected Class<? extends EdgeInputFormat<I, E>>
  edgeInputFormatClass;

  /** Aggregator writer class - cached for fast access */
  protected Class<? extends AggregatorWriter> aggregatorWriterClass;
  /** Combiner class - cached for fast access */
  protected Class<? extends Combiner<I, ? extends Writable>> combinerClass;

  /** Vertex resolver class - cached for fast access */
  protected Class<? extends VertexResolver<I, V, E>> vertexResolverClass;
  /** Worker context class - cached for fast access */
  protected Class<? extends WorkerContext> workerContextClass;
  /** Master compute class - cached for fast access */
  protected Class<? extends MasterCompute> masterComputeClass;

  /** Partition class - cached for fast accesss */
  protected Class<? extends Partition<I, V, E>> partitionClass;

  /** Edge Input Filter class */
  protected Class<? extends EdgeInputFilter<I, E>> edgeInputFilterClass;
  /** Vertex Input Filter class */
  protected Class<? extends VertexInputFilter<I, V, E>>
  vertexInputFilterClass;

  /**
   * Empty constructor. Initialize with default classes or null.
   */
  public GiraphClasses() {
    // Note: the cast to Object is required in order for javac to accept the
    // downcast.
    outEdgesClass = (Class<? extends OutEdges<I, E>>) (Object)
        ByteArrayEdges.class;
    inputOutEdgesClass = (Class<? extends OutEdges<I, E>>) (Object)
        ByteArrayEdges.class;
    vertexValueFactoryClass = (Class<? extends VertexValueFactory<V>>) (Object)
        DefaultVertexValueFactory.class;
    graphPartitionerFactoryClass =
        (Class<? extends GraphPartitionerFactory<I, V, E>>) (Object)
            HashPartitionerFactory.class;
    aggregatorWriterClass = TextAggregatorWriter.class;
    vertexResolverClass = (Class<? extends VertexResolver<I, V, E>>)
        (Object) DefaultVertexResolver.class;
    workerContextClass = DefaultWorkerContext.class;
    masterComputeClass = DefaultMasterCompute.class;
    partitionClass = (Class<? extends Partition<I, V, E>>) (Object)
        SimplePartition.class;
    edgeInputFilterClass = (Class<? extends EdgeInputFilter<I, E>>)
        (Object) DefaultEdgeInputFilter.class;
    vertexInputFilterClass = (Class<? extends VertexInputFilter<I, V, E>>)
        (Object) DefaultVertexInputFilter.class;
  }

  /**
   * Constructor that reads classes from a Configuration object.
   *
   * @param conf Configuration object to read from.
   */
  public GiraphClasses(Configuration conf) {
    readFromConf(conf);
  }

  /**
   * Read classes from Configuration.
   *
   * @param conf Configuration to read from.
   */
  private void readFromConf(Configuration conf) {
    // set pre-validated generic parameter types into Configuration
    computationClass =
        (Class<? extends
            Computation<I, V, E, ? extends Writable, ? extends Writable>>)
            COMPUTATION_CLASS.get(conf);
    List<Class<?>> classList =
        ReflectionUtils.getTypeArguments(Computation.class, computationClass);
    vertexIdClass = (Class<I>) classList.get(0);
    vertexValueClass = (Class<V>) classList.get(1);
    edgeValueClass = (Class<E>) classList.get(2);
    incomingMessageValueClass = (Class<? extends Writable>) classList.get(3);
    outgoingMessageValueClass = (Class<? extends Writable>) classList.get(4);

    outEdgesClass = (Class<? extends OutEdges<I, E>>)
        VERTEX_EDGES_CLASS.get(conf);
    inputOutEdgesClass = (Class<? extends OutEdges<I, E>>)
        INPUT_VERTEX_EDGES_CLASS.getWithDefault(conf, outEdgesClass);
    vertexValueFactoryClass = (Class<? extends VertexValueFactory<V>>)
        VERTEX_VALUE_FACTORY_CLASS.get(conf);

    graphPartitionerFactoryClass =
        (Class<? extends GraphPartitionerFactory<I, V, E>>)
            GRAPH_PARTITIONER_FACTORY_CLASS.get(conf);

    vertexInputFormatClass = (Class<? extends VertexInputFormat<I, V, E>>)
        VERTEX_INPUT_FORMAT_CLASS.get(conf);
    vertexOutputFormatClass = (Class<? extends VertexOutputFormat<I, V, E>>)
        VERTEX_OUTPUT_FORMAT_CLASS.get(conf);
    edgeInputFormatClass = (Class<? extends EdgeInputFormat<I, E>>)
        EDGE_INPUT_FORMAT_CLASS.get(conf);

    aggregatorWriterClass = AGGREGATOR_WRITER_CLASS.get(conf);
    combinerClass = (Class<? extends Combiner<I, ? extends Writable>>)
        VERTEX_COMBINER_CLASS.get(conf);
    vertexResolverClass = (Class<? extends VertexResolver<I, V, E>>)
        VERTEX_RESOLVER_CLASS.get(conf);
    workerContextClass = WORKER_CONTEXT_CLASS.get(conf);
    masterComputeClass =  MASTER_COMPUTE_CLASS.get(conf);
    partitionClass = (Class<? extends Partition<I, V, E>>)
        PARTITION_CLASS.get(conf);

    edgeInputFilterClass = (Class<? extends EdgeInputFilter<I, E>>)
        EDGE_INPUT_FILTER_CLASS.get(conf);
    vertexInputFilterClass = (Class<? extends VertexInputFilter<I, V, E>>)
        VERTEX_INPUT_FILTER_CLASS.get(conf);
  }

  /**
   * Get Computation class
   *
   * @return Computation class.
   */
  public Class<? extends
      Computation<I, V, E, ? extends Writable, ? extends Writable>>
  getComputationClass() {
    return computationClass;
  }

  /**
   * Get Vertex ID class
   *
   * @return Vertex ID class
   */
  public Class<I> getVertexIdClass() {
    return vertexIdClass;
  }

  /**
   * Get Vertex Value class
   *
   * @return Vertex Value class
   */
  public Class<V> getVertexValueClass() {
    return vertexValueClass;
  }

  /**
   * Get Edge Value class
   *
   * @return Edge Value class
   */
  public Class<E> getEdgeValueClass() {
    return edgeValueClass;
  }

  /**
   * Get incoming Message Value class - messages which have been sent in the
   * previous superstep and are processed in the current one
   *
   * @return Message Value class
   */
  public Class<? extends Writable> getIncomingMessageValueClass() {
    return incomingMessageValueClass;
  }

  /**
   * Get outgoing Message Value class - messages which are going to be sent
   * during current superstep
   *
   * @return Message Value class
   */
  public Class<? extends Writable> getOutgoingMessageValueClass() {
    return outgoingMessageValueClass;
  }

  /**
   * Get Vertex edges class
   *
   * @return Vertex edges class.
   */
  public Class<? extends OutEdges<I, E>> getOutEdgesClass() {
    return outEdgesClass;
  }

  /* Get Vertex edges class used during edge-based input
 *
 * @return Vertex edges class.
 */
  public Class<? extends OutEdges<I, E>> getInputOutEdgesClass() {
    return inputOutEdgesClass;
  }


  /**
   * Get vertex value factory class
   *
   * @return Vertex value factory class
   */
  public Class<? extends VertexValueFactory<V>> getVertexValueFactoryClass() {
    return vertexValueFactoryClass;
  }

  /**
   * Get the GraphPartitionerFactory
   *
   * @return GraphPartitionerFactory
   */
  public Class<? extends GraphPartitionerFactory<I, V, E>>
  getGraphPartitionerFactoryClass() {
    return graphPartitionerFactoryClass;
  }

  public Class<? extends EdgeInputFilter<I, E>>
  getEdgeInputFilterClass() {
    return edgeInputFilterClass;
  }

  public Class<? extends VertexInputFilter<I, V, E>>
  getVertexInputFilterClass() {
    return vertexInputFilterClass;
  }

  /**
   * Check if VertexInputFormat class is set
   *
   * @return true if VertexInputFormat class is set
   */
  public boolean hasVertexInputFormat() {
    return vertexInputFormatClass != null;
  }

  /**
   * Get VertexInputFormat held
   *
   * @return VertexInputFormat
   */
  public Class<? extends VertexInputFormat<I, V, E>>
  getVertexInputFormatClass() {
    return vertexInputFormatClass;
  }

  /**
   * Check if VertexOutputFormat is set
   *
   * @return true if VertexOutputFormat is set
   */
  public boolean hasVertexOutputFormat() {
    return vertexOutputFormatClass != null;
  }

  /**
   * Get VertexOutputFormat set
   *
   * @return VertexOutputFormat
   */
  public Class<? extends VertexOutputFormat<I, V, E>>
  getVertexOutputFormatClass() {
    return vertexOutputFormatClass;
  }

  /**
   * Check if EdgeInputFormat is set
   *
   * @return true if EdgeInputFormat is set
   */
  public boolean hasEdgeInputFormat() {
    return edgeInputFormatClass != null;
  }

  /**
   * Get EdgeInputFormat used
   *
   * @return EdgeInputFormat
   */
  public Class<? extends EdgeInputFormat<I, E>> getEdgeInputFormatClass() {
    return edgeInputFormatClass;
  }

  /**
   * Check if AggregatorWriter is set
   *
   * @return true if AggregatorWriter is set
   */
  public boolean hasAggregatorWriterClass() {
    return aggregatorWriterClass != null;
  }

  /**
   * Get AggregatorWriter used
   *
   * @return AggregatorWriter
   */
  public Class<? extends AggregatorWriter> getAggregatorWriterClass() {
    return aggregatorWriterClass;
  }

  /**
   * Check if Combiner is set
   *
   * @return true if Combiner is set
   */
  public boolean hasCombinerClass() {
    return combinerClass != null;
  }

  /**
   * Get Combiner used
   *
   * @return Combiner
   */
  public Class<? extends Combiner<I, ? extends Writable>> getCombinerClass() {
    return combinerClass;
  }

  /**
   * Check if VertexResolver is set
   *
   * @return true if VertexResolver is set
   */
  public boolean hasVertexResolverClass() {
    return vertexResolverClass != null;
  }

  /**
   * Get VertexResolver used
   *
   * @return VertexResolver
   */
  public Class<? extends VertexResolver<I, V, E>> getVertexResolverClass() {
    return vertexResolverClass;
  }

  /**
   * Check if WorkerContext is set
   *
   * @return true if WorkerContext is set
   */
  public boolean hasWorkerContextClass() {
    return workerContextClass != null;
  }

  /**
   * Get WorkerContext used
   *
   * @return WorkerContext
   */
  public Class<? extends WorkerContext> getWorkerContextClass() {
    return workerContextClass;
  }

  /**
   * Check if MasterCompute is set
   *
   * @return true MasterCompute is set
   */
  public boolean hasMasterComputeClass() {
    return masterComputeClass != null;
  }

  /**
   * Get MasterCompute used
   *
   * @return MasterCompute
   */
  public Class<? extends MasterCompute> getMasterComputeClass() {
    return masterComputeClass;
  }

  /**
   * Check if Partition is set
   *
   * @return true if Partition is set
   */
  public boolean hasPartitionClass() {
    return partitionClass != null;
  }

  /**
   * Get Partition
   *
   * @return Partition
   */
  public Class<? extends Partition<I, V, E>> getPartitionClass() {
    return partitionClass;
  }

  /**
   * Set Computation class held, and update message types
   *
   * @param computationClass Computation class to set
   * @return this
   */
  public GiraphClasses setComputationClass(Class<? extends
      Computation<I, V, E, ? extends Writable, ? extends Writable>>
      computationClass) {
    this.computationClass = computationClass;
    List<Class<?>> classList =
        ReflectionUtils.getTypeArguments(Computation.class, computationClass);
    incomingMessageValueClass = (Class<? extends Writable>) classList.get(3);
    outgoingMessageValueClass = (Class<? extends Writable>) classList.get(4);
    return this;
  }

  /**
   * Set Vertex ID class held
   *
   * @param vertexIdClass Vertex ID to set
   * @return this
   */
  public GiraphClasses setVertexIdClass(Class<I> vertexIdClass) {
    this.vertexIdClass = vertexIdClass;
    return this;
  }

  /**
   * Set Vertex Value class held
   *
   * @param vertexValueClass Vertex Value class to set
   * @return this
   */
  public GiraphClasses setVertexValueClass(Class<V> vertexValueClass) {
    this.vertexValueClass = vertexValueClass;
    return this;
  }

  /**
   * Set Edge Value class held
   *
   * @param edgeValueClass Edge Value class to set
   * @return this
   */
  public GiraphClasses setEdgeValueClass(Class<E> edgeValueClass) {
    this.edgeValueClass = edgeValueClass;
    return this;
  }

  /**
   * Set incoming Message Value class held - messages which have been sent in
   * the previous superstep and are processed in the current one
   *
   * @param incomingMessageValueClass Message Value class to set
   * @return this
   */
  public GiraphClasses setIncomingMessageValueClass(
      Class<? extends Writable> incomingMessageValueClass) {
    this.incomingMessageValueClass = incomingMessageValueClass;
    return this;
  }

  /**
   * Set outgoing Message Value class held - messages which are going to be sent
   * during current superstep
   *
   * @param outgoingMessageValueClass Message Value class to set
   * @return this
   */
  public GiraphClasses setOutgoingMessageValueClass(
      Class<? extends Writable> outgoingMessageValueClass) {
    this.outgoingMessageValueClass = outgoingMessageValueClass;
    return this;
  }

  /**
   * Set OutEdges class held
   *
   * @param outEdgesClass Vertex edges class to set
   * @return this
   */
  public GiraphClasses setOutEdgesClass(
      Class<? extends OutEdges> outEdgesClass) {
    this.outEdgesClass =
        (Class<? extends OutEdges<I, E>>) outEdgesClass;
    return this;
  }

  /**
   * Set OutEdges class used during edge-input (if different from the one
   * used for computation)
   *
   * @param inputOutEdgesClass Input vertex edges class to set
   * @return this
   */
  public GiraphClasses setInputOutEdgesClass(
      Class<? extends OutEdges> inputOutEdgesClass) {
    this.inputOutEdgesClass =
        (Class<? extends OutEdges<I, E>>) inputOutEdgesClass;
    return this;
  }

  /**
   * Set VertexValueFactory class held
   *
   * @param vertexValueFactoryClass Vertex value factory class to set
   * @return this
   */
  public GiraphClasses setVertexValueFactoryClass(
      Class<? extends VertexValueFactory> vertexValueFactoryClass) {
    this.vertexValueFactoryClass = (Class<? extends VertexValueFactory<V>>)
        vertexValueFactoryClass;
    return this;
  }

  /**
   * Set GraphPartitionerFactory class held
   *
   * @param klass GraphPartitionerFactory to set
   * @return this
   */
  public GiraphClasses setGraphPartitionerFactoryClass(
      Class<? extends GraphPartitionerFactory<I, V, E>> klass) {
    this.graphPartitionerFactoryClass = klass;
    return this;
  }

  /**
   * Set VertexInputFormat held
   *
   * @param vertexInputFormatClass VertexInputFormat to set
   * @return this
   */
  public GiraphClasses setVertexInputFormatClass(
      Class<? extends VertexInputFormat<I, V, E>> vertexInputFormatClass) {
    this.vertexInputFormatClass = vertexInputFormatClass;
    return this;
  }

  /**
   * Set VertexOutputFormat held
   *
   * @param vertexOutputFormatClass VertexOutputFormat to set
   * @return this
   */
  public GiraphClasses setVertexOutputFormatClass(
      Class<? extends VertexOutputFormat<I, V, E>> vertexOutputFormatClass) {
    this.vertexOutputFormatClass = vertexOutputFormatClass;
    return this;
  }

  /**
   * Set EdgeInputFormat class held
   *
   * @param edgeInputFormatClass EdgeInputFormat to set
   * @return this
   */
  public GiraphClasses setEdgeInputFormatClass(
      Class<? extends EdgeInputFormat<I, E>> edgeInputFormatClass) {
    this.edgeInputFormatClass = edgeInputFormatClass;
    return this;
  }

  /**
   * Set AggregatorWriter class used
   *
   * @param aggregatorWriterClass AggregatorWriter to set
   * @return this
   */
  public GiraphClasses setAggregatorWriterClass(
      Class<? extends AggregatorWriter> aggregatorWriterClass) {
    this.aggregatorWriterClass = aggregatorWriterClass;
    return this;
  }

  /**
   * Set Combiner class used
   *
   * @param combinerClass Combiner class to set
   * @return this
   */
  public GiraphClasses setCombinerClass(
      Class<? extends Combiner<I, ? extends Writable>> combinerClass) {
    this.combinerClass = combinerClass;
    return this;
  }

  /**
   * Set VertexResolver used
   *
   * @param vertexResolverClass VertexResolver to set
   * @return this
   */
  public GiraphClasses setVertexResolverClass(
      Class<? extends VertexResolver<I, V, E>> vertexResolverClass) {
    this.vertexResolverClass = vertexResolverClass;
    return this;
  }

  /**
   * Set WorkerContext used
   *
   * @param workerContextClass WorkerContext class to set
   * @return this
   */
  public GiraphClasses setWorkerContextClass(
      Class<? extends WorkerContext> workerContextClass) {
    this.workerContextClass = workerContextClass;
    return this;
  }

  /**
   * Set MasterCompute class used
   *
   * @param masterComputeClass MasterCompute class to set
   * @return this
   */
  public GiraphClasses setMasterComputeClass(
      Class<? extends MasterCompute> masterComputeClass) {
    this.masterComputeClass = masterComputeClass;
    return this;
  }

  /**
   * Set Partition class to use
   *
   * @param partitionClass Partition class to set
   * @return this
   */
  public GiraphClasses setPartitionClass(
      Class<? extends Partition<I, V, E>> partitionClass) {
    this.partitionClass = partitionClass;
    return this;
  }
}
