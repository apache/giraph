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

package org.apache.giraph.block_app.library.pagerank;

import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration options for PageRank algorithm
 */
public class PageRankSettings {
  /** Number of iterations. */
  public static final IntConfOption ITERATIONS = new IntConfOption(
      "giraph.pagerank.iterations", 10, "Number of iterations");
  /** Damping factor. */
  public static final FloatConfOption DAMPING_FACTOR = new FloatConfOption(
      "giraph.pagerank.dampingFactor", 0.85f, "Damping factor");
  /** Initial PageRank value. */
  public static final FloatConfOption INITIAL_VALUE = new FloatConfOption(
      "giraph.pagerank.initialValue", 1.0f, "Initial value");
  /**
   * Convergence criteria, default is no convergence checking.
   * See {@link PageRankConvergenceType} for various types.
   */
  public static final StrConfOption CONVERGENCE_TYPE = new StrConfOption(
      "giraph.pagerank.convergenceType",
      PageRankConvergenceType.NO_CONVERGENCE.toString(),
      "Convergence criteria, default is no convergence checking");
  /** The threshold error for convergence */
  public static final FloatConfOption CONVERGENCE_THRESHOLD =
      new FloatConfOption(
          "giraph.pagerank.convergenceThreshold", 0.00001f,
          "The threshold error for convergence");
  /** Whether we are using weighted or unweighted graph */
  public static final BooleanConfOption WEIGHTED_PAGERANK =
      new BooleanConfOption("giraph.pagerank.weighted", true,
          "Whether to run weighted or unweighted pagerank");

  /** Don't construct */
  protected PageRankSettings() { }

  /**
   * Get number of iterations
   *
   * @param conf Configuration
   * @return num iterations
   */
  public static int getIterations(Configuration conf) {
    return ITERATIONS.get(conf);
  }

  /**
   * Get damping factor
   *
   * @param conf Configuration
   * @return daping factor
   */
  public static double getDampingFactor(Configuration conf) {
    return DAMPING_FACTOR.get(conf);
  }

  /**
   * Get initial value
   *
   * @param conf Configuration
   * @return initial value
   */
  public static double getInitialValue(Configuration conf) {
    return INITIAL_VALUE.get(conf);
  }

  /**
   * Get the type of convergence
   *
   * @param conf Configuration
   * @return The type of convergence
   */
  public static PageRankConvergenceType getConvergenceType(Configuration conf) {
    return PageRankConvergenceType.valueOf(CONVERGENCE_TYPE.get(conf));
  }

  /**
   * Get the convergence threshold error
   *
   * @param conf Configuration
   * @return The convergence threshold
   */
  public static float getConvergenceThreshold(Configuration conf) {
    return CONVERGENCE_THRESHOLD.get(conf);
  }

  /**
   * Check whether to use weighted or unweighted pagerank
   *
   * @param conf Configuration
   * @return Whether to use weighted or unweighted pagerank
   */
  public static boolean isWeighted(Configuration conf) {
    return WEIGHTED_PAGERANK.get(conf);
  }
}
