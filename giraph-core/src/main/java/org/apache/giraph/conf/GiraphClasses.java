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
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.factories.ComputationFactory;
import org.apache.giraph.factories.DefaultComputationFactory;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.DefaultVertexValueCombiner;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.MappingInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.filters.DefaultEdgeInputFilter;
import org.apache.giraph.io.filters.DefaultVertexInputFilter;
import org.apache.giraph.io.filters.EdgeInputFilter;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.HashPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.SimplePartition;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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
  /** ComputationFactory class - cached for fast access */
  protected Class<? extends ComputationFactory<I, V, E,
      ? extends Writable, ? extends Writable>>
  computationFactoryClass;
  /** Computation class - cached for fast access */
  protected Class<? extends Computation<I, V, E,
      ? extends Writable, ? extends Writable>>
  computationClass;
  /** Generic types used to describe graph */
  protected GiraphTypes<I, V, E> giraphTypes;
  /** Vertex edges class - cached for fast access */
  protected Class<? extends OutEdges<I, E>> outEdgesClass;
  /** Input vertex edges class - cached for fast access */
  protected Class<? extends OutEdges<I, E>> inputOutEdgesClass;

  /** Graph partitioner factory class - cached for fast access */
  protected Class<? extends GraphPartitionerFactory<I, V, E>>
  graphPartitionerFactoryClass;

  /** Vertex input format class - cached for fast access */
  protected Class<? extends VertexInputFormat<I, V, E>>
  vertexInputFormatClass;
  /** Vertex output format class - cached for fast access */
  protected Class<? extends VertexOutputFormat<I, V, E>>
  vertexOutputFormatClass;
  /** Mapping input format - cached for fast access */
  protected Class<? extends MappingInputFormat<I, V, E, ? extends Writable>>
  mappingInputFormatClass;
  /** Edge input format class - cached for fast access */
  protected Class<? extends EdgeInputFormat<I, E>>
  edgeInputFormatClass;
  /** Edge output format class - cached for fast access */
  protected Class<? extends EdgeOutputFormat<I, V, E>>
  edgeOutputFormatClass;

  /** Aggregator writer class - cached for fast access */
  protected Class<? extends AggregatorWriter> aggregatorWriterClass;

  /** Incoming message classes */
  protected MessageClasses<I, ? extends Writable> incomingMessageClasses;
  /** Outgoing message classes */
  protected MessageClasses<I, ? extends Writable> outgoingMessageClasses;

  /** Vertex resolver class - cached for fast access */
  protected Class<? extends VertexResolver<I, V, E>> vertexResolverClass;
  /** Vertex value combiner class - cached for fast access */
  protected Class<? extends VertexValueCombiner<V>> vertexValueCombinerClass;
  /** Worker context class - cached for fast access */
  protected Class<? extends WorkerContext> workerContextClass;
  /** Master compute class - cached for fast access */
  protected Class<? extends MasterCompute> masterComputeClass;

  /** Partition class - cached for fast accesss */
  protected Class<? extends Partition<I, V, E>> partitionClass;

  /** Edge Input Filter class */
  protected Class<? extends EdgeInputFilter<I, E>> edgeInputFilterClass;
  /** Vertex Input Filter class */
  protected Class<? extends VertexInputFilter<I, V, E>> vertexInputFilterClass;

  /**
   * Empty constructor. Initialize with default classes or null.
   */
  public GiraphClasses() {
    computationFactoryClass = (Class) DefaultComputationFactory.class;
    giraphTypes = new GiraphTypes<I, V, E>();
    outEdgesClass = (Class) ByteArrayEdges.class;
    inputOutEdgesClass = (Class) ByteArrayEdges.class;
    graphPartitionerFactoryClass = (Class) HashPartitionerFactory.class;
    aggregatorWriterClass = TextAggregatorWriter.class;
    vertexResolverClass = (Class) DefaultVertexResolver.class;
    vertexValueCombinerClass = (Class) DefaultVertexValueCombiner.class;
    workerContextClass = DefaultWorkerContext.class;
    masterComputeClass = DefaultMasterCompute.class;
    partitionClass = (Class) SimplePartition.class;
    edgeInputFilterClass = (Class) DefaultEdgeInputFilter.class;
    vertexInputFilterClass = (Class) DefaultVertexInputFilter.class;
  }

  /**
   * Constructor that reads classes from a Configuration object.
   *
   * @param conf Configuration object to read from.
   */
  public GiraphClasses(Configuration conf) {
    giraphTypes = GiraphTypes.readFrom(conf);
    computationFactoryClass =
        (Class<? extends ComputationFactory<I, V, E,
            ? extends Writable, ? extends Writable>>)
            COMPUTATION_FACTORY_CLASS.get(conf);
    computationClass =
        (Class<? extends Computation<I, V, E,
            ? extends Writable, ? extends Writable>>)
            COMPUTATION_CLASS.get(conf);

    outEdgesClass = (Class<? extends OutEdges<I, E>>)
        VERTEX_EDGES_CLASS.get(conf);
    inputOutEdgesClass = (Class<? extends OutEdges<I, E>>)
        INPUT_VERTEX_EDGES_CLASS.getWithDefault(conf, outEdgesClass);

    graphPartitionerFactoryClass =
        (Class<? extends GraphPartitionerFactory<I, V, E>>)
            GRAPH_PARTITIONER_FACTORY_CLASS.get(conf);

    vertexInputFormatClass = (Class<? extends VertexInputFormat<I, V, E>>)
        VERTEX_INPUT_FORMAT_CLASS.get(conf);
    vertexOutputFormatClass = (Class<? extends VertexOutputFormat<I, V, E>>)
        VERTEX_OUTPUT_FORMAT_CLASS.get(conf);
    edgeInputFormatClass = (Class<? extends EdgeInputFormat<I, E>>)
        EDGE_INPUT_FORMAT_CLASS.get(conf);
    edgeOutputFormatClass = (Class<? extends EdgeOutputFormat<I, V, E>>)
        EDGE_OUTPUT_FORMAT_CLASS.get(conf);
    mappingInputFormatClass = (Class<? extends MappingInputFormat<I, V, E,
        ? extends Writable>>)
        MAPPING_INPUT_FORMAT_CLASS.get(conf);

    aggregatorWriterClass = AGGREGATOR_WRITER_CLASS.get(conf);

    // incoming messages shouldn't be used in first iteration at all
    // but empty message stores are created, etc, so using NoMessage
    // to enforce not a single message is read/written
    incomingMessageClasses = new DefaultMessageClasses(
        NoMessage.class,
        DefaultMessageValueFactory.class,
        null,
        MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION);
    outgoingMessageClasses = new DefaultMessageClasses(
        giraphTypes.getInitialOutgoingMessageValueClass(),
        OUTGOING_MESSAGE_VALUE_FACTORY_CLASS.get(conf),
        MESSAGE_COMBINER_CLASS.get(conf),
        MESSAGE_ENCODE_AND_STORE_TYPE.get(conf));

    vertexResolverClass = (Class<? extends VertexResolver<I, V, E>>)
        VERTEX_RESOLVER_CLASS.get(conf);
    vertexValueCombinerClass = (Class<? extends VertexValueCombiner<V>>)
        VERTEX_VALUE_COMBINER_CLASS.get(conf);
    workerContextClass = WORKER_CONTEXT_CLASS.get(conf);
    masterComputeClass =  MASTER_COMPUTE_CLASS.get(conf);
    partitionClass = (Class<? extends Partition<I, V, E>>)
        PARTITION_CLASS.get(conf);

    edgeInputFilterClass = (Class<? extends EdgeInputFilter<I, E>>)
        EDGE_INPUT_FILTER_CLASS.get(conf);
    vertexInputFilterClass = (Class<? extends VertexInputFilter<I, V, E>>)
        VERTEX_INPUT_FILTER_CLASS.get(conf);
  }

  public Class<? extends ComputationFactory<I, V, E,
      ? extends Writable, ? extends Writable>> getComputationFactoryClass() {
    return computationFactoryClass;
  }

  /**
   * Get Computation class
   *
   * @return Computation class.
   */
  public Class<? extends Computation<I, V, E,
      ? extends Writable, ? extends Writable>>
  getComputationClass() {
    return computationClass;
  }

  public GiraphTypes<I, V, E> getGiraphTypes() {
    return giraphTypes;
  }

  /**
   * Get Vertex ID class
   *
   * @return Vertex ID class
   */
  public Class<I> getVertexIdClass() {
    return giraphTypes.getVertexIdClass();
  }


  /**
   * Get Vertex implementation class
   *
   * @return Vertex implementation class
   */
  public Class<? extends Vertex> getVertexClass() {
    return giraphTypes.getVertexClass();
  }


  /**
   * Get Vertex Value class
   *
   * @return Vertex Value class
   */
  public Class<V> getVertexValueClass() {
    return giraphTypes.getVertexValueClass();
  }

  /**
   * Get Edge Value class
   *
   * @return Edge Value class
   */
  public Class<E> getEdgeValueClass() {
    return giraphTypes.getEdgeValueClass();
  }


  public MessageClasses<? extends WritableComparable, ? extends Writable>
  getIncomingMessageClasses() {
    return incomingMessageClasses;
  }

  public MessageClasses<? extends WritableComparable, ? extends Writable>
  getOutgoingMessageClasses() {
    return outgoingMessageClasses;
  }

  /**
   * Get Vertex edges class
   *
   * @return Vertex edges class.
   */
  public Class<? extends OutEdges<I, E>> getOutEdgesClass() {
    return outEdgesClass;
  }

  /**
   * Get Vertex edges class used during edge-based input
   *
   * @return Vertex edges class.
   */
  public Class<? extends OutEdges<I, E>> getInputOutEdgesClass() {
    return inputOutEdgesClass;
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
   * Check if MappingInputFormat is set
   *
   * @return true if MappingInputFormat is set
   */
  public boolean hasMappingInputFormat() {
    return mappingInputFormatClass != null;
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

  public Class<? extends MappingInputFormat<I, V, E, ? extends Writable>>
  getMappingInputFormatClass() {
    return mappingInputFormatClass;
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
   * Check if EdgeOutputFormat is set
   *
   * @return true if EdgeOutputFormat is set
   */
  public boolean hasEdgeOutputFormat() {
    return edgeOutputFormatClass != null;
  }

  /**
   * Get VertexOutputFormat set
   *
   * @return VertexOutputFormat
   */
  public Class<? extends EdgeOutputFormat<I, V, E>>
  getEdgeOutputFormatClass() {
    return edgeOutputFormatClass;
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
   * Get VertexValueCombiner used
   *
   * @return VertexValueCombiner
   */
  public Class<? extends VertexValueCombiner<V>> getVertexValueCombinerClass() {
    return vertexValueCombinerClass;
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
    return this;
  }

  /**
   * Set Vertex ID class held
   *
   * @param vertexIdClass Vertex ID to set
   * @return this
   */
  public GiraphClasses setVertexIdClass(Class<I> vertexIdClass) {
    giraphTypes.setVertexIdClass(vertexIdClass);
    return this;
  }

  /**
   * Set Vertex Value class held
   *
   * @param vertexValueClass Vertex Value class to set
   * @return this
   */
  public GiraphClasses setVertexValueClass(Class<V> vertexValueClass) {
    giraphTypes.setVertexValueClass(vertexValueClass);
    return this;
  }

  /**
   * Set Edge Value class held
   *
   * @param edgeValueClass Edge Value class to set
   * @return this
   */
  public GiraphClasses setEdgeValueClass(Class<E> edgeValueClass) {
    giraphTypes.setEdgeValueClass(edgeValueClass);
    return this;
  }

  /**
   * Set incoming Message Value class held - messages which have been sent in
   * the previous superstep and are processed in the current one
   *
   * @param incomingMessageClasses Message classes value to set
   * @return this
   */
  public GiraphClasses setIncomingMessageClasses(
      MessageClasses<I, ? extends Writable> incomingMessageClasses) {
    this.incomingMessageClasses = incomingMessageClasses;
    return this;
  }

  /**
   * Set outgoing Message Value class held - messages which are going to be sent
   * during current superstep
   *
   * @param outgoingMessageClasses Message classes value to set
   * @return this
   */
  public GiraphClasses setOutgoingMessageClasses(
      MessageClasses<I, ? extends Writable> outgoingMessageClasses) {
    this.outgoingMessageClasses = outgoingMessageClasses;
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
   * Set MappingInputFormat held
   *
   * @param mappingInputFormatClass MappingInputFormat to set
   * @return this
   */
  public GiraphClasses setMappingInputFormatClass(
    Class<? extends MappingInputFormat<I, V, E, Writable>>
      mappingInputFormatClass) {
    this.mappingInputFormatClass = mappingInputFormatClass;
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
