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

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Immutable global state of the graph.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class GraphState<I extends WritableComparable, V extends Writable,
E extends Writable, M extends Writable> {
  /** Graph-wide superstep */
  private final long superstep;
  /** Graph-wide number of vertices */
  private final long numVertices;
  /** Graph-wide number of edges */
  private final long numEdges;
  /** Graph-wide map context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Graph-wide BSP Mapper for this Vertex */
  private final GraphTaskManager<I, V, E, M> graphTaskManager;
  /** Handles requests */
  private final WorkerClientRequestProcessor<I, V, E, M>
  workerClientRequestProcessor;
  /** Worker aggregator usage */
  private final WorkerAggregatorUsage workerAggregatorUsage;
  /** Partition context */
  private PartitionContext partitionContext;

  /**
   * Constructor
   *
   * @param superstep Current superstep
   * @param numVertices Current graph-wide vertices
   * @param numEdges Current graph-wide edges
   * @param context Context
   * @param graphTaskManager GraphTaskManager for this compute node
   * @param workerClientRequestProcessor Handles all communication
   * @param workerAggregatorUsage Aggregator usage
   *
   */
  public GraphState(
      long superstep, long numVertices,
      long numEdges, Mapper<?, ?, ?, ?>.Context context,
      GraphTaskManager<I, V, E, M> graphTaskManager,
      WorkerClientRequestProcessor<I, V, E, M> workerClientRequestProcessor,
      WorkerAggregatorUsage workerAggregatorUsage) {
    this.superstep = superstep;
    this.numVertices = numVertices;
    this.numEdges = numEdges;
    this.context = context;
    this.graphTaskManager = graphTaskManager;
    this.workerClientRequestProcessor = workerClientRequestProcessor;
    this.workerAggregatorUsage = workerAggregatorUsage;
  }

  public long getSuperstep() {
    return superstep;
  }

  public long getTotalNumVertices() {
    return numVertices;
  }

  public long getTotalNumEdges() {
    return numEdges;
  }

  public Mapper.Context getContext() {
    return context;
  }

  public GraphTaskManager<I, V, E, M> getGraphTaskManager() {
    return graphTaskManager;
  }

  public WorkerClientRequestProcessor<I, V, E, M>
  getWorkerClientRequestProcessor() {
    return workerClientRequestProcessor;
  }

  public WorkerAggregatorUsage getWorkerAggregatorUsage() {
    return workerAggregatorUsage;
  }

  public void setPartitionContext(PartitionContext partitionContext) {
    this.partitionContext = partitionContext;
  }

  public PartitionContext getPartitionContext() {
    return partitionContext;
  }

  @Override
  public String toString() {
    return "(superstep=" + superstep + ",numVertices=" + numVertices + "," +
        "numEdges=" + numEdges + ",context=" + context +
        ",graphMapper=" + graphTaskManager +
        ",workerClientRequestProcessor=" + workerClientRequestProcessor + ")";

  }
}
