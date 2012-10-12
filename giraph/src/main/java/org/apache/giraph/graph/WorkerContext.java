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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WorkerContext allows for the execution of user code
 * on a per-worker basis. There's one WorkerContext per worker.
 */
@SuppressWarnings("rawtypes")
public abstract class WorkerContext implements WorkerAggregatorUsage {
  /** Global graph state */
  private GraphState graphState;

  /**
   * Set the graph state.
   *
   *  @param graphState Used to set the graph state.
   */
  public void setGraphState(GraphState graphState) {
    this.graphState = graphState;
  }

  /**
   * Initialize the WorkerContext.
   * This method is executed once on each Worker before the first
   * superstep starts.
   *
   * @throws IllegalAccessException Thrown for getting the class
   * @throws InstantiationException Expected instantiation in this method.
   */
  public abstract void preApplication() throws InstantiationException,
    IllegalAccessException;

  /**
   * Finalize the WorkerContext.
   * This method is executed once on each Worker after the last
   * superstep ends.
   */
  public abstract void postApplication();

  /**
   * Execute user code.
   * This method is executed once on each Worker before each
   * superstep starts.
   */
  public abstract void preSuperstep();

  /**
   * Execute user code.
   * This method is executed once on each Worker after each
   * superstep ends.
   */
  public abstract void postSuperstep();

  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  public long getSuperstep() {
    return graphState.getSuperstep();
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  public long getTotalNumVertices() {
    return graphState.getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  public long getTotalNumEdges() {
    return graphState.getTotalNumEdges();
  }

  /**
   * Get the mapper context
   *
   * @return Mapper context
   */
  public Mapper.Context getContext() {
    return graphState.getContext();
  }

  @Override
  public <A extends Writable> void aggregate(String name, A value) {
    graphState.getGraphMapper().getWorkerAggregatorUsage().
        aggregate(name, value);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return graphState.getGraphMapper().getWorkerAggregatorUsage().
        <A>getAggregatedValue(name);
  }
}
