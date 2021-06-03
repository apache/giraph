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

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Immutable global state of the graph.
 */
public class GraphState {
  /** Graph-wide superstep */
  private final long superstep;
  /** Graph-wide number of vertices */
  private final long numVertices;
  /** Graph-wide number of edges */
  private final long numEdges;
  /** Graph-wide map context */
  private final Mapper<?, ?, ?, ?>.Context context;

  /**
   * Constructor
   *
   * @param superstep Current superstep
   * @param numVertices Current graph-wide vertices
   * @param numEdges Current graph-wide edges
   * @param context Context
   *
   */
  public GraphState(long superstep, long numVertices, long numEdges,
      Mapper<?, ?, ?, ?>.Context context) {
    this.superstep = superstep;
    this.numVertices = numVertices;
    this.numEdges = numEdges;
    this.context = context;
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

  @Override
  public String toString() {
    return "(superstep=" + superstep + ",numVertices=" + numVertices + "," +
        "numEdges=" + numEdges + ",context=" + context + ")";
  }
}
