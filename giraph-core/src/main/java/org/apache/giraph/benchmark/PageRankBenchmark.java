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
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.combiner.DoubleSumCombiner;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.edge.LongDoubleArrayEdges;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.io.formats.JsonBase64VertexOutputFormat;
import org.apache.giraph.io.formats.PseudoRandomEdgeInputFormat;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.giraph.partition.SimpleLongRangePartitionerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Default Pregel-style PageRank computation.
 */
public class PageRankBenchmark implements Tool {
  /**
   * Class logger
   */
  private static final Logger LOG = Logger.getLogger(PageRankBenchmark.class);
  /**
   * Configuration from Configurable
   */
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public final int run(final String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("v", "verbose", false, "Verbose");
    options.addOption("w",
        "workers",
        true,
        "Number of workers");
    options.addOption("s",
        "supersteps",
        true,
        "Supersteps to execute before finishing");
    options.addOption("V",
        "aggregateVertices",
        true,
        "Aggregate vertices");
    options.addOption("e",
        "edgesPerVertex",
        true,
        "Edges per vertex");
    options.addOption("c",
        "edgesClass",
        true,
        "Vertex edges class (0 for LongDoubleArrayEdges," +
            "1 for ByteArrayEdges, " +
            "2 for ByteArrayEdges with unsafe serialization, " +
            "3 for ArrayListEdges, " +
            "4 for HashMapVertex");
    options.addOption("ei",
        "edgeInput",
        false,
        "Use edge-based input instead of vertex-based input.");
    options.addOption("l",
        "localEdgesMinRatio",
        true,
        "Minimum ratio of partition-local edges (default is 0)");
    options.addOption("p",
        "partitioner",
        true,
        "Partitioning algorithm (0 for hash partitioning (default), " +
            "1 for range partitioning)");
    options.addOption("N",
        "name",
        true,
        "Name of the job");
    options.addOption("t",
        "combinerType",
        true,
        "Combiner type (0 for no combiner, 1 for DoubleSumCombiner (default)");
    options.addOption("o",
        "vertexOutputFormat",
        true,
        "0 for JsonBase64VertexOutputFormat");

    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption('h')) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    if (!cmd.hasOption('w')) {
      LOG.info("Need to choose the number of workers (-w)");
      return -1;
    }
    if (!cmd.hasOption('s')) {
      LOG.info("Need to set the number of supersteps (-s)");
      return -1;
    }
    if (!cmd.hasOption('V')) {
      LOG.info("Need to set the aggregate vertices (-V)");
      return -1;
    }
    if (!cmd.hasOption('e')) {
      LOG.info("Need to set the number of edges " +
          "per vertex (-e)");
      return -1;
    }

    int workers = Integer.parseInt(cmd.getOptionValue('w'));
    String name = getClass().getName();
    if (cmd.hasOption("N")) {
      name = name + " " + cmd.getOptionValue("N");
    }

    GiraphJob job = new GiraphJob(getConf(), name);
    GiraphConfiguration configuration = job.getConfiguration();
    setClassesAndParameters(cmd, configuration);

    configuration.setWorkerConfiguration(workers, workers, 100.0f);
    configuration.setInt(
        PageRankVertex.SUPERSTEP_COUNT,
        Integer.parseInt(cmd.getOptionValue('s')));

    boolean isVerbose = false;
    if (cmd.hasOption('v')) {
      isVerbose = true;
    }
    if (job.run(isVerbose)) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Set vertex edges, input format, partitioner classes and related parameters
   * based on command-line arguments.
   *
   * @param cmd Command line arguments
   * @param configuration Giraph job configuration
   */
  protected void setClassesAndParameters(
      CommandLine cmd, GiraphConfiguration configuration) {
    configuration.setVertexClass(PageRankVertex.class);
    int edgesClassOption = cmd.hasOption('c') ? Integer.parseInt(
        cmd.getOptionValue('c')) : 1;
    switch (edgesClassOption) {
    case 0:
      configuration.setVertexEdgesClass(LongDoubleArrayEdges.class);
      break;
    case 1:
      configuration.setVertexEdgesClass(ByteArrayEdges.class);
      break;
    case 2:
      configuration.setVertexEdgesClass(ByteArrayEdges.class);
      configuration.useUnsafeSerialization(true);
      break;
    case 3:
      configuration.setVertexEdgesClass(ArrayListEdges.class);
      break;
    case 4:
      configuration.setVertexEdgesClass(HashMapEdges.class);
      break;
    default:
      LOG.info("Unknown VertexEdges class, " +
          "defaulting to LongDoubleArrayEdges");
      configuration.setVertexEdgesClass(LongDoubleArrayEdges.class);
    }

    LOG.info("Using edges class " +
        configuration.get(GiraphConstants.VERTEX_EDGES_CLASS));
    if (!cmd.hasOption('t') ||
        (Integer.parseInt(cmd.getOptionValue('t')) == 1)) {
      configuration.setVertexCombinerClass(DoubleSumCombiner.class);
    }

    if (cmd.hasOption("ei")) {
      configuration.setEdgeInputFormatClass(PseudoRandomEdgeInputFormat.class);
    } else {
      configuration.setVertexInputFormatClass(
          PseudoRandomVertexInputFormat.class);
    }

    configuration.setLong(
        PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        Long.parseLong(cmd.getOptionValue('V')));
    configuration.setLong(
        PseudoRandomInputFormatConstants.EDGES_PER_VERTEX,
        Long.parseLong(cmd.getOptionValue('e')));
    if (cmd.hasOption('l')) {
      float localEdgesMinRatio = Float.parseFloat(cmd.getOptionValue('l'));
      configuration.setFloat(
          PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
          localEdgesMinRatio);
    }

    int vertexOutputClassOption =
        cmd.hasOption('o') ? Integer.parseInt(cmd.getOptionValue('o')) : -1;
    if (vertexOutputClassOption == 0) {
      LOG.info("Using vertex output format class " +
          JsonBase64VertexOutputFormat.class.getName());
      configuration.setVertexOutputFormatClass(
          JsonBase64VertexOutputFormat.class);
    }

    if (cmd.hasOption('p') &&
        Integer.parseInt(cmd.getOptionValue('p')) == 1) {
      configuration.setGraphPartitionerFactoryClass(
          SimpleLongRangePartitionerFactory.class);
    }
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new PageRankBenchmark(), args));
  }
}
