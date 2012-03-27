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
package org.apache.giraph;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Helper class to run Giraph applications by specifying the actual class name
 * to use (i.e. vertex, vertex input/output format, combiner, etc.).
 */
public class GiraphRunner implements Tool {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphRunner.class);
  /** Writable conf */
  private Configuration conf;

  /**
   * Required options.
   */
  private final String [][] requiredOptions =
  {{"w", "Need to choose the number of workers (-w)"},
   {"if", "Need to set inputformat (-if)"}};

  /**
   * Get the options available.
   *
   * @return Options available.
   */
  private static Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("q", "quiet", false, "Quiet output");
    options.addOption("w", "workers", true, "Number of workers");
    options.addOption("if", "inputFormat", true, "Graph inputformat");
    options.addOption("of", "outputFormat", true, "Graph outputformat");
    options.addOption("ip", "inputPath", true, "Graph input path");
    options.addOption("op", "outputPath", true, "Graph output path");
    options.addOption("c", "combiner", true, "VertexCombiner class");
    options.addOption("wc", "workerContext", true, "WorkerContext class");
    options.addOption("aw", "aggregatorWriter", true, "AggregatorWriter class");
    options.addOption("ca", "customArguments", true, "provide custom" +
        " arguments for the job configuration in the form:" +
        " <param1>=<value1>,<param2>=<value2> etc.");
    return options;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = getOptions();
    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    String vertexClassName = args[0];
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to run Vertex: " + vertexClassName);
    }

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    // Verify all the options have been provided
    for (String[] requiredOption : requiredOptions) {
      if (!cmd.hasOption(requiredOption[0])) {
        if (LOG.isInfoEnabled()) {
          LOG.info(requiredOption[1]);
        }
        return -1;
      }
    }

    int workers = Integer.parseInt(cmd.getOptionValue('w'));
    GiraphJob job = new GiraphJob(getConf(), "Giraph: " + vertexClassName);
    job.setVertexClass(Class.forName(vertexClassName));
    job.setVertexInputFormatClass(Class.forName(cmd.getOptionValue("if")));
    job.setVertexOutputFormatClass(Class.forName(cmd.getOptionValue("of")));

    if (cmd.hasOption("ip")) {
      FileInputFormat.addInputPath(job.getInternalJob(),
                                   new Path(cmd.getOptionValue("ip")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No input path specified. Ensure your InputFormat does" +
            " not require one.");
      }
    }

    if (cmd.hasOption("op")) {
      FileOutputFormat.setOutputPath(job.getInternalJob(),
                                     new Path(cmd.getOptionValue("op")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No output path specified. Ensure your OutputFormat does" +
            " not require one.");
      }
    }

    if (cmd.hasOption("c")) {
      job.setVertexCombinerClass(Class.forName(cmd.getOptionValue("c")));
    }

    if (cmd.hasOption("wc")) {
      job.setWorkerContextClass(Class.forName(cmd.getOptionValue("wc")));
    }

    if (cmd.hasOption("aw")) {
      job.setAggregatorWriterClass(Class.forName(cmd.getOptionValue("aw")));
    }

    if (cmd.hasOption("ca")) {
      Configuration jobConf = job.getConfiguration();
      for (String paramValue :
          Splitter.on(',').split(cmd.getOptionValue("ca"))) {
        String[] parts = Iterables.toArray(Splitter.on('=').split(paramValue),
            String.class);
        if (parts.length != 2) {
          throw new IllegalArgumentException("Unable to parse custom " +
              " argument: " + paramValue);
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("Setting custom argument [" + parts[0] + "] to [" +
              parts[1] + "]");
        }
        jobConf.set(parts[0], parts[1]);
      }
    }

    job.setWorkerConfiguration(workers, workers, 100.0f);

    boolean isQuiet = !cmd.hasOption('q');

    return job.run(isQuiet) ? 0 : -1;
  }

  /**
   * Execute GiraphRunner.
   *
   * @param args Typically command line arguments.
   * @throws Exception Any exceptions thrown.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GiraphRunner(), args));
  }
}
