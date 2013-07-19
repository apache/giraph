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

package org.apache.giraph.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.combiner.MinimumDoubleMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Single-source shortest paths benchmark.
 */
public class ShortestPathsBenchmark extends GiraphBenchmark {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ShortestPathsBenchmark.class);

  /** Option for OutEdges class */
  private static final BenchmarkOption EDGES_CLASS = new BenchmarkOption(
      "c", "edgesClass", true,
      "Vertex edges class (0 for HashMapEdges, 1 for ArrayListEdges)");
  /** Option for not using combiner */
  private static final BenchmarkOption NO_COMBINER = new BenchmarkOption(
      "nc", "noCombiner", false, "Don't use a combiner");

  @Override
  public Set<BenchmarkOption> getBenchmarkOptions() {
    return Sets.newHashSet(BenchmarkOption.VERTICES,
        BenchmarkOption.EDGES_PER_VERTEX, EDGES_CLASS, NO_COMBINER);
  }

  @Override
  protected void prepareConfiguration(GiraphConfiguration conf,
      CommandLine cmd) {
    conf.setComputationClass(ShortestPathsComputation.class);
    if (EDGES_CLASS.getOptionIntValue(cmd, 1) == 1) {
      conf.setOutEdgesClass(ArrayListEdges.class);
    } else {
      conf.setOutEdgesClass(HashMapEdges.class);
    }
    LOG.info("Using class " + GiraphConstants.COMPUTATION_CLASS.get(conf));
    conf.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    if (!NO_COMBINER.optionTurnedOn(cmd)) {
      conf.setMessageCombinerClass(MinimumDoubleMessageCombiner.class);
    }
    conf.setLong(PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        BenchmarkOption.VERTICES.getOptionLongValue(cmd));
    conf.setLong(PseudoRandomInputFormatConstants.EDGES_PER_VERTEX,
        BenchmarkOption.EDGES_PER_VERTEX.getOptionLongValue(cmd));
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new ShortestPathsBenchmark(), args));
  }
}
