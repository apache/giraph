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

import org.apache.giraph.bsp.checkpoints.CheckpointStatus;

/**
 * Immutable graph stats after the completion of a superstep
 */
public class FinishedSuperstepStats extends VertexEdgeCount {
  /** Number of local vertices */
  private final long localVertexCount;
  /** Are all the graph vertices halted? */
  private final boolean allVerticesHalted;
  /** Needs to load a checkpoint */
  private final boolean mustLoadCheckpoint;
  /**
   * Master decides when we need to checkpoint and what should
   * we do next.
   */
  private final CheckpointStatus checkpointStatus;

  /**
   * Constructor.
   *
   * @param numLocalVertices Number of local vertices
   * @param allVerticesHalted Are all the vertices halted
   * @param numVertices Number of vertices
   * @param numEdges Number of edges
   * @param mustLoadCheckpoint Has to load a checkpoint?
   * @param checkpointStatus Should we checkpoint after this superstep?
   */
  public FinishedSuperstepStats(long numLocalVertices,
                                boolean allVerticesHalted,
                                long numVertices,
                                long numEdges,
                                boolean mustLoadCheckpoint,
                                CheckpointStatus checkpointStatus) {
    super(numVertices, numEdges, 0);
    this.localVertexCount = numLocalVertices;
    this.allVerticesHalted = allVerticesHalted;
    this.mustLoadCheckpoint = mustLoadCheckpoint;
    this.checkpointStatus = checkpointStatus;
  }

  public long getLocalVertexCount() {
    return localVertexCount;
  }

  /**
   * Are all the vertices halted?
   *
   * @return True if all halted, false otherwise
   */
  public boolean allVerticesHalted() {
    return allVerticesHalted;
  }

  /**
   * Must load the checkpoint?
   *
   * @return True if the checkpoint must be loaded, false otherwise
   */
  public boolean mustLoadCheckpoint() {
    return mustLoadCheckpoint;
  }

  /**
   * What master thinks about checkpointing after this superstep.
   * @return CheckpointStatus that reflects master decision.
   */
  public CheckpointStatus getCheckpointStatus() {
    return checkpointStatus;
  }
}
