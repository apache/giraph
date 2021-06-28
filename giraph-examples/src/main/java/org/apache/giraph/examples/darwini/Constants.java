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
package org.apache.giraph.examples.darwini;

import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.conf.StrConfOption;

/**
 * Graph generation constants
 */
public class Constants {
  /**
   * The number of vertices in the graph
   */
  public static final LongConfOption AGGREGATE_VERTICES =
      new LongConfOption(
          "digraph.graphgeneration.aggregate.vertices", 0,
          "Number vertices in the graph");

  /**
   * Number of splits used for global optimization.
   */
  public static final IntConfOption AGGREGATORS_SPLITS =
      new IntConfOption("digraph.graphgeneration.aggregators.splits", 100,
          "Number of aggregator splits");


  /**
   * Random edge requests to send in each superstep.
   */
  public static final IntConfOption RANDOM_EDGE_REQUESTS_PER_SUPERSTEP =
      new IntConfOption("digraph.graphgeneration.req.per.superstep", 10,
          "How many random edge requests should we send");

  /**
   * How many random edge supersteps should we run in each iteration.
   */
  public static final IntConfOption RANDOM_EDGE_REQUEST_SUPERSTEPS =
      new IntConfOption("digraph.graphgeneration.random.supersteps", 100,
          "How many random edge request supersteps should we run?");

  /**
   * Max number of iterations.
   */
  public static final IntConfOption MAX_ITERATIONS =
      new IntConfOption("digraph.graphgeneration.max.iterations", 1000,
          "Max number of iterations");

  /**
   * Number of random iterations
   */
  public static final IntConfOption RANDOM_ITERATIONS =
      new IntConfOption("digraph.graphgeneration.random.iterations", 5,
          "Max number of iterations");

  /**
   * Ratio of edges we want to create with random community
   * supersteps.
   */
  public static final FloatConfOption GEO_RATIO =
      new FloatConfOption("digraph.graphgeneration.geo.ratio", 1f,
          "Percentage of edges participating in geo edge part");

  /**
   * Should we use random communities at all?
   */
  public static final IntConfOption USE_RANDOM_GEO =
      new IntConfOption("digraph.graphgeneration.use.random.geo", 1,
          "Use random community edges");

  /**
   * Starting ID for multi-stage graph generation.
   */
  public static final LongConfOption ID_BASE =
      new LongConfOption("digraph.graphgeneration.id.base", 0,
          "First vertex ID");

  /**
   * Boundaries between super-communities for multi-stage generation.
   */
  public static final StrConfOption BOUNDARIES =
      new StrConfOption("digraph.graphgeneration.boundaries", "",
          "Splits boundaries");

  /**
   * Max size of the community for random edge creation.
   */
  public static final IntConfOption MAX_GEO_SPAN =
      new IntConfOption("digraph.graphgeneration.max.geo.span", 16,
          "Max geo span");

  /**
   * Ratio of edges created withtin the super-community for
   * multi-stage graph generation.
   */
  public static final FloatConfOption IN_COUNTRY_PERCENTAGE =
      new FloatConfOption("digraph.graphgeneration.in.country.percentage",
          0.84f,
          "Percentage of edges occuring within the country");

  /**
   * Source file with distribution
   */
  public static final StrConfOption FILE_TO_LOAD_FROM =
      new StrConfOption("digraph.graphgeneration.source.file", "dblp.csv",
          "Source file with distributions");

  /** Default constructor */
  private Constants() { }
}
