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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Main application of Darwini graph generator.
 */
public class GraphGenerator implements Tool {

  /**
   * Logger
   */
  private static final Logger LOG =
      Logger.getLogger(LoadFromFileDistributions.class);


  /**
   * Giraph job configuration
   */
  private Configuration conf;

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("w",
        "workers",
        true,
        "Minimum number of workers");
    options.addOption("o",
        "outputDirectory",
        true,
        "Output directory");
    options.addOption("v",
        "vertices",
        true,
        "Number of vertices to generate");
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
    if (!cmd.hasOption('o')) {
      LOG.info("Need to set the output directory (-o)");
      return -1;
    }
    if (!cmd.hasOption('v')) {
      LOG.info("Need to set desired number of vertices (-v)");
      return -1;
    }
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    BlockUtils.setAndInitBlockFactoryClass(job.getConfiguration(),
        GraphGenerationFactory.class);
    Constants.AGGREGATE_VERTICES.set(job.getConfiguration(),
        Integer.parseInt(cmd.getOptionValue('v')));
    Constants.AGGREGATORS_SPLITS.set(job.getConfiguration(), 1);
    Constants.FILE_TO_LOAD_FROM.set(job.getConfiguration(), "fb.csv");
    GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(
        job.getConfiguration(),
        GraphGenerationVertexInputFormat.class);
    GiraphConstants.NUM_OUTPUT_THREADS.set(job.getConfiguration(), 1);
    GiraphConstants.VERTEX_OUTPUT_FORMAT_THREAD_SAFE.set(
        job.getConfiguration(), false);
    job.getConfiguration().setWorkerConfiguration(
        Integer.parseInt(cmd.getOptionValue('w')),
        Integer.parseInt(cmd.getOptionValue('w')),
        100.0f);
    job.getConfiguration().setVertexOutputFormatClass(
        EdgeOutputFormat.class);

    FileOutputFormat.setOutputPath(job.getInternalJob(),
        new Path(cmd.getOptionValue('o')));


    if (job.run(true)) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Main function. Entry point of application.
   * @param args application parameters
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GraphGenerator(), args));
  }
}
