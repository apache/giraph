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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

/**
 * Base class for executing a random walk on a graph
 *
 * @param <E> edge type
 */
public abstract class RandomWalkVertex<E extends Writable>
    extends Vertex<LongWritable, DoubleWritable, E, DoubleWritable> {
  /** Configuration parameter for the number of supersteps to execute */
  static final String MAX_SUPERSTEPS = RandomWalkVertex.class.getName() +
      ".maxSupersteps";
  /** Configuration parameter for the teleportation probability */
  static final String TELEPORTATION_PROBABILITY = RandomWalkVertex.class
      .getName() + ".teleportationProbability";
  /** Name of aggregator for the probability of dangling vertices */
  static final String CUMULATIVE_DANGLING_PROBABILITY = RandomWalkVertex.class
      .getName() + ".cumulativeDanglingProbability";
  /** Name of aggregator for the probability of all vertices */
  static final String CUMULATIVE_PROBABILITY = RandomWalkVertex.class
      .getName() + ".cumulativeProbability";
    /** Name of aggregator for the probability of dangling vertices */
  static final String NUM_DANGLING_VERTICES = RandomWalkVertex.class
      .getName() + ".numDanglingVertices";
  /** Name of aggregator for the L1 norm of the probability difference, used
   * for covergence detection */
  static final String L1_NORM_OF_PROBABILITY_DIFFERENCE = RandomWalkVertex.class
      .getName() + ".l1NormOfProbabilityDifference";
  /** Reusable {@link DoubleWritable} instance to avoid object instantiation */
  private final DoubleWritable doubleWritable = new DoubleWritable();
  /** Reusable {@link LongWritable} for counting dangling vertices */
  private final LongWritable one = new LongWritable(1);

  /**
   * Compute an initial probability value for the vertex. Per default,
   * we start with a uniform distribution.
   * @return The initial probability value.
   */
  protected double initialProbability() {
    return 1.0 / getTotalNumVertices();
  }

  /**
   * Compute the probability of transitioning to a neighbor vertex
   * @param stateProbability current steady state probability of the vertex
   * @param edge edge to neighbor
   * @return the probability of transitioning to a neighbor vertex
   */
  protected abstract double transitionProbability(double stateProbability,
      Edge<LongWritable, E> edge);

  /**
   * Perform a single step of a random walk computation.
   * @param messages Messages received in the previous step.
   * @param teleportationProbability Probability of teleporting to another
   *          vertex.
   * @return The new probability distribution value.
   */
  protected abstract double recompute(Iterable<DoubleWritable> messages,
      double teleportationProbability);

  /**
   * Returns the cumulative probability from dangling vertices.
   * @return The cumulative probability from dangling vertices.
   */
  protected double getDanglingProbability() {
    return this.<DoubleWritable>getAggregatedValue(
        RandomWalkVertex.CUMULATIVE_DANGLING_PROBABILITY).get();
  }

  /**
   * Returns the cumulative probability from dangling vertices.
   * @return The cumulative probability from dangling vertices.
   */
  protected double getPreviousCumulativeProbability() {
    return this.<DoubleWritable>getAggregatedValue(
        RandomWalkVertex.CUMULATIVE_PROBABILITY).get();
  }

  @Override
  public void compute(Iterable<DoubleWritable> messages) throws IOException {
    double stateProbability;

    if (getSuperstep() > 0) {

      double previousStateProbability = getValue().get();
      stateProbability = recompute(messages, teleportationProbability());

      // Important: rescale for numerical stability
      stateProbability /= getPreviousCumulativeProbability();

      doubleWritable.set(Math.abs(stateProbability - previousStateProbability));
      aggregate(L1_NORM_OF_PROBABILITY_DIFFERENCE, doubleWritable);

    } else {
      stateProbability = initialProbability();
    }

    getValue().set(stateProbability);

    aggregate(CUMULATIVE_PROBABILITY, getValue());

    // Compute dangling node contribution for next superstep
    if (getNumEdges() == 0) {
      aggregate(NUM_DANGLING_VERTICES, one);
      aggregate(CUMULATIVE_DANGLING_PROBABILITY, getValue());
    }

    if (getSuperstep() < maxSupersteps()) {
      for (Edge<LongWritable, E> edge : getEdges()) {
        double transitionProbability =
            transitionProbability(stateProbability, edge);
        doubleWritable.set(transitionProbability);
        sendMessage(edge.getTargetVertexId(), doubleWritable);
      }
    } else {
      voteToHalt();
    }
  }

  /**
   * Reads the number of supersteps to execute from the configuration
   * @return number of supersteps to execute
   */
  private int maxSupersteps() {
    return ((RandomWalkWorkerContext) getWorkerContext()).getMaxSupersteps();
  }

  /**
   * Reads the teleportation probability from the configuration
   * @return teleportation probability
   */
  protected double teleportationProbability() {
    return ((RandomWalkWorkerContext) getWorkerContext())
        .getTeleportationProbability();
  }
}
