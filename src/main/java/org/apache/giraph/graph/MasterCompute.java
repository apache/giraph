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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Interface for defining a master vertex that can perform centralized
 * computation between supersteps. This class will be instantiated on the
 * master node and will run every superstep before the workers do.
 *
 * Communication with the workers should be performed via aggregators. The
 * values of the aggregators are broadcast to the workers before
 * vertex.compute() is called and collected by the master before
 * master.compute() is called. This means aggregator values used by the workers
 * are consistent with aggregator values from the master from the same
 * superstep and aggregator used by the master are consistent with aggregator
 * values from the workers from the previous superstep. Note that the master
 * has to register its own aggregators (it does not call {@link WorkerContext}
 * functions), but it uses all aggregators by default, so useAggregator does
 * not have to be called.
 */
@SuppressWarnings("rawtypes")
public abstract class MasterCompute implements MasterAggregatorUsage, Writable,
    Configurable {
  /** If true, do not do anymore computation on this vertex. */
  private boolean halt = false;
  /** Global graph state **/
  private GraphState graphState;
  /** Configuration */
  private Configuration conf;

  /**
   * Must be defined by user to specify what the master has to do.
   */
  public abstract void compute();

  /**
   * Initialize the MasterCompute class, this is the place to register
   * aggregators.
   */
  public abstract void initialize() throws InstantiationException,
    IllegalAccessException;

  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  public long getSuperstep() {
    return getGraphState().getSuperstep();
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  public long getTotalNumVertices() {
    return getGraphState().getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  public long getTotalNumEdges() {
    return getGraphState().getTotalNumEdges();
  }

  /**
   * After this is called, the computation will stop, even if there are
   * still messages in the system or vertices that have not voted to halt.
   */
  public void haltComputation() {
    halt = true;
  }

  /**
   * Has the master halted?
   *
   * @return True if halted, false otherwise.
   */
  public boolean isHalted() {
    return halt;
  }

  /**
   * Get the graph state for all workers.
   *
   * @return Graph state for all workers
   */
  GraphState getGraphState() {
    return graphState;
  }

  /**
   * Set the graph state for all workers
   *
   * @param graphState Graph state for all workers
   */
  void setGraphState(GraphState graphState) {
    this.graphState = graphState;
  }

  /**
   * Get the mapper context
   *
   * @return Mapper context
   */
  public Mapper.Context getContext() {
    return getGraphState().getContext();
  }

  @Override
  public final <A extends Writable> boolean registerAggregator(
    String name, Class<? extends Aggregator<A>> aggregatorClass)
    throws InstantiationException, IllegalAccessException {
    return getGraphState().getGraphMapper().getMasterAggregatorUsage().
        registerAggregator(name, aggregatorClass);
  }

  @Override
  public final <A extends Writable> boolean registerPersistentAggregator(
      String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    return getGraphState().getGraphMapper().getMasterAggregatorUsage().
        registerPersistentAggregator(name, aggregatorClass);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return getGraphState().getGraphMapper().getMasterAggregatorUsage().
        <A>getAggregatedValue(name);
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    getGraphState().getGraphMapper().getMasterAggregatorUsage().
        setAggregatedValue(name, value);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
