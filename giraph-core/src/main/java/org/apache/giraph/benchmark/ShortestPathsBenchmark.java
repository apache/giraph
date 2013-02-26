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
import org.apache.giraph.combiner.MinimumDoubleCombiner;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ArrayListEdges;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomVertexInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Single-source shortest paths benchmark.
 */
public class ShortestPathsBenchmark implements Tool {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ShortestPathsBenchmark.class);
  /** Configuration */
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
        "Vertex edges class (0 for HashMapEdges, 1 for ArrayListEdges)");
    options.addOption("nc",
        "noCombiner",
        false,
        "Don't use a combiner");
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
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.getConfiguration().setVertexClass(ShortestPathsVertex.class);
    if (!cmd.hasOption('c') ||
        (Integer.parseInt(cmd.getOptionValue('c')) == 1)) {
      job.getConfiguration().setVertexEdgesClass(ArrayListEdges.class);
    } else {
      job.getConfiguration().setVertexEdgesClass(HashMapEdges.class);
    }
    LOG.info("Using class " +
        job.getConfiguration().get(GiraphConstants.VERTEX_CLASS));
    job.getConfiguration().setVertexInputFormatClass(
        PseudoRandomVertexInputFormat.class);
    if (!cmd.hasOption("nc")) {
      job.getConfiguration().setVertexCombinerClass(
          MinimumDoubleCombiner.class);
    }
    job.getConfiguration().setWorkerConfiguration(workers, workers, 100.0f);
    job.getConfiguration().setLong(
        PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        Long.parseLong(cmd.getOptionValue('V')));
    job.getConfiguration().setLong(
        PseudoRandomInputFormatConstants.EDGES_PER_VERTEX,
        Long.parseLong(cmd.getOptionValue('e')));

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
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new ShortestPathsBenchmark(), args));
  }
}
