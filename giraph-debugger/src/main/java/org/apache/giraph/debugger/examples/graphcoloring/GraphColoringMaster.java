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
package org.apache.giraph.debugger.examples.graphcoloring;

import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * MasterCompute for graph coloring algorithm.
 */
public class GraphColoringMaster extends DefaultMasterCompute {

  /**
   * Aggregator name for phase.
   */
  public static final String PHASE = "phase";
  /**
   * Aggregator name for color to assign.
   */
  public static final String COLOR_TO_ASSIGN = "colorToAssign";
  /**
   * Aggregator name for number of colored vertices.
   */
  public static final String NUM_VERTICES_COLORED = "numVerticesColored";
  /**
   * Aggregator name for number of unknown vertices.
   */
  public static final String NUM_VERTICES_UNKNOWN = "numVerticesUnknown";
  /**
   * Aggregator name for number of vertices in the independent set.
   */
  public static final String NUM_VERTICES_IN_SET = "numVerticesInSet";
  /**
   * Aggregator name for number of vertices not in the independent set.
   */
  public static final String NUM_VERTICES_NOT_IN_SET = "numVerticesNotInSet";
  /**
   * Aggregator name for number of vertices tentatively in the independent set.
   */
  public static final String NUM_VERTICES_TENTATIVELY_IN_SET =
    "numVerticesTentativelyInSet";

  /**
   * Phases in the graph coloring algorithm.
   */
  public static enum Phase {
    /**
     * The phase we select unknown vertices to put into tentatively in-set.
     */
    LOTTERY,
    /**
     * The phase we resolve conflicts between neighboring tentatively
     * in-set vertices.
     */
    CONFLICT_RESOLUTION,
    /**
     * The phase we remove edges of in-set vertices.
     */
    EDGE_CLEANING,
    /**
     * The phase we assign colors to the in-set vertices.
     */
    COLOR_ASSIGNMENT,
  }

  /**
   * Current color to assign.
   */
  private int colorToAssign;
  /**
   * Current phase.
   */
  private Phase phase;

  @Override
  public void initialize() throws InstantiationException,
    IllegalAccessException {
    registerPersistentAggregator(COLOR_TO_ASSIGN, IntMaxAggregator.class);
    colorToAssign = VertexValue.NO_COLOR;
    registerPersistentAggregator(PHASE, IntMaxAggregator.class);
    phase = null;

    registerPersistentAggregator(NUM_VERTICES_COLORED, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_UNKNOWN, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_TENTATIVELY_IN_SET,
      LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_NOT_IN_SET, LongSumAggregator.class);
    registerAggregator(NUM_VERTICES_IN_SET, LongSumAggregator.class);
  }

  @Override
  public void compute() {
    if (phase != null) {
      switch (phase) {
      case LOTTERY:
        // Move to conflict resolution after selecting a set of vertices.
        phase = Phase.CONFLICT_RESOLUTION;
        break;

      case CONFLICT_RESOLUTION:
        // After resolving conflicts, move on to edge cleaning.
        phase = Phase.EDGE_CLEANING;
        break;

      case EDGE_CLEANING:
        // We can assign colors to the vertices in the independent set if there
        // are no remaining UNKNOWNs at a LOTTERY phase.
        long numUnknown = ((LongWritable) getAggregatedValue(
            NUM_VERTICES_UNKNOWN)).get();
        if (numUnknown == 0) {
          // Set an aggregator telling each IN_SET vertex what color to assign.
          setAggregatedValue(COLOR_TO_ASSIGN, new IntWritable(++colorToAssign));
          phase = Phase.COLOR_ASSIGNMENT;
        } else {
          // Repeat finding independent sets after cleaning edges.
          // remaining.
          phase = Phase.LOTTERY;
        }
        break;

      case COLOR_ASSIGNMENT:
        long numColored = ((LongWritable) getAggregatedValue(
            NUM_VERTICES_COLORED)).get();
        if (numColored == getTotalNumVertices()) {
          // Halt when all vertices are colored.
          haltComputation();
          return;
        }
        // Start a new cycle of finding maximal independent sets, after
        // assigning colors.
        phase = Phase.LOTTERY;
        break;

      default:
        throw new IllegalStateException();
      }
    } else {
      // First superstep, enter into lottery.
      phase = Phase.LOTTERY;
    }

    // Set an aggregator to communicate what phase we're in to all vertices.
    setAggregatedValue(PHASE, new IntWritable(phase.ordinal()));
  }
}
