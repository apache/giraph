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

package org.apache.giraph.examples;

import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.DoubleWritable;

/**
 * Executes "RandomWalkWithRestart", a random walk on the graph which is biased
 * towards a source vertex. The resulting probabilities of staying at a given
 * vertex can be interpreted as a measure of proximity to the source vertex.
 */
public class RandomWalkWithRestartVertex extends RandomWalkVertex {

  /** Configuration parameter for the source vertex */
  static final String SOURCE_VERTEX = RandomWalkWithRestartVertex.class
      .getName() + ".sourceVertex";

  /**
   * Checks whether the currently executed vertex is the source vertex
   * @return is the currently executed vertex the source vertex?
   */
  private boolean isSourceVertex() {
    return ((RandomWalkWorkerContext) getWorkerContext()).isSource(getId()
        .get());
  }

  /**
   * Returns the number of source vertexes.
   * @return The number of source vertexes.
   */
  private int numSourceVertexes() {
    return ((RandomWalkWorkerContext) getWorkerContext()).numSources();
  }

  /**
   * Returns the cumulated probability from dangling nodes.
   * @return The cumulated probability from dangling nodes.
   */
  private double getDanglingProbability() {
    return this.<DoubleWritable>getAggregatedValue(RandomWalkVertex.DANGLING)
        .get();
  }

  /**
   * Start with a uniform distribution.
   * @return A uniform probability over all the vertexces.
   */
  @Override
  protected double initialProbability() {
    return 1.0 / getTotalNumVertices();
  }

  @Override
  protected double recompute(Iterable<DoubleWritable> transitionProbabilities,
      double teleportationProbability) {
    double stateProbability = MathUtils.sum(transitionProbabilities);
    // Add the contribution of dangling nodes (weakly preferential
    // implementation: dangling nodes redistribute uniformly)
    stateProbability += getDanglingProbability() / getTotalNumVertices();
    // The random walk might teleport back to one of the source vertexes
    stateProbability *= 1 - teleportationProbability;
    if (isSourceVertex()) {
      stateProbability += teleportationProbability / numSourceVertexes();
    }
    return stateProbability;
  }
}
