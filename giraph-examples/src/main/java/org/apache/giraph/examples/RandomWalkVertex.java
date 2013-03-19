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

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

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
  /** Name of aggregator for collecting the probability of dangling vertices */
  static final String CUMULATIVE_DANGLING_PROBABILITY = RandomWalkVertex.class
      .getName() + ".cumulativeDanglingProbability";
  /** Name of aggregator for the L1 norm of the probability difference, used
   * for covergence detection */
  static final String L1_NORM_OF_PROBABILITY_DIFFERENCE = RandomWalkVertex.class
      .getName() + ".l1NormOfProbabilityDifference";
  /** Logger */
  private static final Logger LOG = Logger.getLogger(RandomWalkVertex.class);
  /** Reusable {@link DoubleWritable} instance to avoid object instantiation */
  private final DoubleWritable doubleWritable = new DoubleWritable();

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
   * Returns the cumulative probability from dangling nodes.
   * @return The cumulative probability from dangling nodes.
   */
  protected double getDanglingProbability() {
    return this.<DoubleWritable>getAggregatedValue(
        RandomWalkVertex.CUMULATIVE_DANGLING_PROBABILITY).get();
  }

  @Override
  public void compute(Iterable<DoubleWritable> messages) throws IOException {
    double stateProbability;

    if (getSuperstep() > 0) {
      double previousStateProbability = getValue().get();
      stateProbability = recompute(messages, teleportationProbability());

      doubleWritable.set(Math.abs(stateProbability - previousStateProbability));
      aggregate(L1_NORM_OF_PROBABILITY_DIFFERENCE, doubleWritable);

    } else {
      stateProbability = initialProbability();
    }
    doubleWritable.set(stateProbability);
    setValue(doubleWritable);

    // Compute dangling node contribution for next superstep
    if (getNumEdges() == 0) {
      aggregate(CUMULATIVE_DANGLING_PROBABILITY, doubleWritable);
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

  /**
   * Master compute associated with {@link RandomWalkVertex}. It handles
   * dangling nodes.
   */
  public static class RandomWalkVertexMasterCompute extends
      DefaultMasterCompute {

    /** threshold for the L1 norm of the state vector difference  */
    static final double CONVERGENCE_THRESHOLD = 0.00001;

    @Override
    public void compute() {
      double danglingContribution =
          this.<DoubleWritable>getAggregatedValue(
          RandomWalkVertex.CUMULATIVE_DANGLING_PROBABILITY).get();
      double l1NormOfStateDiff =
          this.<DoubleWritable>getAggregatedValue(
          RandomWalkVertex.L1_NORM_OF_PROBABILITY_DIFFERENCE).get();

      LOG.info("[Superstep " + getSuperstep() + "] Dangling contribution = " +
          danglingContribution + ", L1 Norm of state vector difference = " +
          l1NormOfStateDiff);

      // Convergence check: halt once the L1 norm of the difference between the
      // state vectors fall under the threshold
      if (getSuperstep() > 1 && l1NormOfStateDiff < CONVERGENCE_THRESHOLD) {
        haltComputation();
      }

    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(RandomWalkVertex.CUMULATIVE_DANGLING_PROBABILITY,
          DoubleSumAggregator.class);
      registerAggregator(RandomWalkVertex.L1_NORM_OF_PROBABILITY_DIFFERENCE,
          DoubleSumAggregator.class);
    }
  }
}
