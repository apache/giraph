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

import java.io.IOException;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.graph.DefaultMasterCompute;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.LongDoubleFloatDoubleEdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Base class for executing a random walk on the graph
 */
public abstract class RandomWalkVertex extends
    LongDoubleFloatDoubleEdgeListVertex {
  /** Configuration parameter for the number of supersteps to execute */
  static final String MAX_SUPERSTEPS = RandomWalkVertex.class.getName() +
      ".maxSupersteps";
  /** Configuration parameter for the teleportation probability */
  static final String TELEPORTATION_PROBABILITY = RandomWalkVertex.class
      .getName() + ".teleportationProbability";
  /** Name of aggregator for dangling nodes */
  static final String DANGLING = "dangling";
  /** Logger */
  private static final Logger LOG = Logger.getLogger(RandomWalkVertex.class);
  /** State probability of the vertex */
  protected final DoubleWritable d = new DoubleWritable();

  /**
   * Compute an initial probability distribution for the vertex.
   * @return The initial probability value.
   */
  protected abstract double initialProbability();

  /**
   * Perform a single step of a random walk computation.
   * @param messages Messages received in the previous step.
   * @param teleportationProbability Probability of teleporting to another
   *          vertex.
   * @return The new probability distribution value.
   */
  protected abstract double recompute(Iterable<DoubleWritable> messages,
      double teleportationProbability);

  @Override
  public void compute(Iterable<DoubleWritable> messages) throws IOException {
    double stateProbability;

    if (getSuperstep() > 0) {
      stateProbability = recompute(messages, teleportationProbability());
    } else {
      stateProbability = initialProbability();
    }
    d.set(stateProbability);
    setValue(d);

    // Compute dangling node contribution for next superstep
    if (getNumEdges() == 0) {
      aggregate(DANGLING, d);
    }

    // Execute the algorithm as often as configured,
    // alternatively convergence could be checked via an Aggregator
    if (getSuperstep() < maxSupersteps()) {
      for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
        double transitionProbability = stateProbability * edge.getValue().get();
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(
            transitionProbability));
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
    @Override
    public void compute() {
      // TODO This is a good place to implement halting by checking convergence.
      double danglingContribution =
          this.<DoubleWritable>getAggregatedValue(RandomWalkVertex.DANGLING)
              .get();
      LOG.info("[Superstep " + getSuperstep() + "] Dangling contribution = " +
          danglingContribution);
    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(RandomWalkVertex.DANGLING, DoubleSumAggregator.class);
    }
  }
}
