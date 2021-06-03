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
import org.apache.giraph.combiner.DoubleSumMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.edge.LongDoubleArrayEdges;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.JsonBase64VertexOutputFormat;
import org.apache.giraph.io.formats.PseudoRandomEdgeInputFormat;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.giraph.partition.SimpleLongRangePartitionerFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Benchmark for {@link WeightedPageRankComputation}
 */
public class WeightedPageRankBenchmark extends GiraphBenchmark {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(WeightedPageRankBenchmark.class);

  /** Option for OutEdges class */
  private static final BenchmarkOption EDGES_CLASS = new BenchmarkOption(
      "c", "edgesClass", true,
      "Vertex edges class (0 for LongDoubleArrayEdges," +
          "1 for ByteArrayEdges, " +
          "2 for ByteArrayEdges with unsafe serialization, " +
          "3 for ArrayListEdges, " +
          "4 for HashMapVertex");
  /** Option for using edge input */
  private static final BenchmarkOption EDGE_INPUT = new BenchmarkOption(
      "ei", "edgeInput", false,
      "Use edge-based input instead of vertex-based input.");
  /** Option for partitioning algorithm */
  private static final BenchmarkOption PARTITIONER = new BenchmarkOption(
      "p", "partitioner", true,
      "Partitioning algorithm (0 for hash partitioning (default), " +
          "1 for range partitioning)");
  /** Option for type of combiner */
  private static final BenchmarkOption MESSAGE_COMBINER_TYPE =
      new BenchmarkOption("t", "combinerType", true,
          "MessageCombiner type (0 for no combiner," +
              " 1 for DoubleSumMessageCombiner (default)");
  /** Option for output format */
  private static final BenchmarkOption OUTPUT_FORMAT = new BenchmarkOption(
      "o", "vertexOutputFormat", true,
      "0 for JsonBase64VertexOutputFormat");

  @Override
  public Set<BenchmarkOption> getBenchmarkOptions() {
    return Sets.newHashSet(
        BenchmarkOption.SUPERSTEPS, BenchmarkOption.VERTICES,
        BenchmarkOption.EDGES_PER_VERTEX, BenchmarkOption.LOCAL_EDGES_MIN_RATIO,
        EDGES_CLASS, EDGE_INPUT, PARTITIONER,
        MESSAGE_COMBINER_TYPE, OUTPUT_FORMAT);
  }

  /**
   * Set vertex edges, input format, partitioner classes and related parameters
   * based on command-line arguments.
   *
   * @param cmd           Command line arguments
   * @param configuration Giraph job configuration
   */
  protected void prepareConfiguration(GiraphConfiguration configuration,
      CommandLine cmd) {
    configuration.setComputationClass(WeightedPageRankComputation.class);
    int edgesClassOption = EDGES_CLASS.getOptionIntValue(cmd, 1);
    switch (edgesClassOption) {
    case 0:
      configuration.setOutEdgesClass(LongDoubleArrayEdges.class);
      break;
    case 1:
      configuration.setOutEdgesClass(ByteArrayEdges.class);
      break;
    case 2:
      configuration.setOutEdgesClass(ByteArrayEdges.class);
      configuration.useUnsafeSerialization(true);
      break;
    case 3:
      configuration.setOutEdgesClass(ArrayListEdges.class);
      break;
    case 4:
      configuration.setOutEdgesClass(HashMapEdges.class);
      break;
    default:
      LOG.info("Unknown OutEdges class, " +
          "defaulting to LongDoubleArrayEdges");
      configuration.setOutEdgesClass(LongDoubleArrayEdges.class);
    }

    LOG.info("Using edges class " +
        GiraphConstants.VERTEX_EDGES_CLASS.get(configuration));
    if (MESSAGE_COMBINER_TYPE.getOptionIntValue(cmd, 1) == 1) {
      configuration.setMessageCombinerClass(DoubleSumMessageCombiner.class);
    }

    if (EDGE_INPUT.optionTurnedOn(cmd)) {
      configuration.setEdgeInputFormatClass(PseudoRandomEdgeInputFormat.class);
    } else {
      configuration.setVertexInputFormatClass(
          PseudoRandomVertexInputFormat.class);
    }

    configuration.setLong(
        PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        BenchmarkOption.VERTICES.getOptionLongValue(cmd));
    configuration.setLong(
        PseudoRandomInputFormatConstants.EDGES_PER_VERTEX,
        BenchmarkOption.EDGES_PER_VERTEX.getOptionLongValue(cmd));
    configuration.setFloat(
        PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
        BenchmarkOption.LOCAL_EDGES_MIN_RATIO.getOptionFloatValue(cmd,
            PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO_DEFAULT));

    if (OUTPUT_FORMAT.getOptionIntValue(cmd, -1) == 0) {
      LOG.info("Using vertex output format class " +
          JsonBase64VertexOutputFormat.class.getName());
      configuration.setVertexOutputFormatClass(
          JsonBase64VertexOutputFormat.class);
    }

    if (PARTITIONER.getOptionIntValue(cmd, 0) == 1) {
      configuration.setGraphPartitionerFactoryClass(
          SimpleLongRangePartitionerFactory.class);
    }

    configuration.setInt(WeightedPageRankComputation.SUPERSTEP_COUNT,
        BenchmarkOption.SUPERSTEPS.getOptionIntValue(cmd));
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new WeightedPageRankBenchmark(), args));
  }
}
