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

/**
 * Immutable results of finishSuperste()
 */
public class FinishedSuperstepStats extends VertexEdgeCount {
  /** Are all the graph vertices halted? */
  private final boolean allVerticesHalted;

  /**
   * Constructor.
   *
   * @param allVerticesHalted Are all the vertices halted
   * @param numVertices Number of vertices
   * @param numEdges Number of edges
   */
  public FinishedSuperstepStats(boolean allVerticesHalted,
                                long numVertices,
                                long numEdges) {
    super(numVertices, numEdges);
    this.allVerticesHalted = allVerticesHalted;
  }

  public boolean getAllVerticesHalted() {
    return allVerticesHalted;
  }
}
