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
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.job.GiraphTypeValidator;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.AnnotationUtils;
import org.apache.giraph.vertex.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.net.URI;
import java.util.List;

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
   * Get the options available.
   *
   * @return Options available.
   */
  private static Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("la", "listAlgorithms", false, "List supported " +
        "algorithms");
    options.addOption("q", "quiet", false, "Quiet output");
    options.addOption("w", "workers", true, "Number of workers");
    options.addOption("vif", "vertexInputFormat", true, "Vertex input format");
    options.addOption("eif", "edgeInputFormat", true, "Edge input format");
    options.addOption("of", "outputFormat", true, "Vertex output format");
    options.addOption("vip", "vertexInputPath", true, "Vertex input path");
    options.addOption("eip", "edgeInputPath", true, "Edge input path");
    options.addOption("op", "outputPath", true, "Vertex output path");
    options.addOption("c", "combiner", true, "Combiner class");
    options.addOption("wc", "workerContext", true, "WorkerContext class");
    options.addOption("aw", "aggregatorWriter", true, "AggregatorWriter class");
    options.addOption("mc", "masterCompute", true, "MasterCompute class");
    options.addOption("cf", "cacheFile", true, "Files for distributed cache");
    options.addOption("ca", "customArguments", true, "provide custom" +
        " arguments for the job configuration in the form:" +
        " -ca <param1>=<value1>,<param2>=<value2> -ca <param3>=<value3> etc." +
        " It can appear multiple times, and the last one has effect" +
        " for the same param.");
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

  /**
   * Prints description of algorithms annotated with {@link Algorithm}
   */
  private void printSupportedAlgorithms() {
    Logger.getLogger(ZooKeeper.class).setLevel(Level.OFF);

    List<Class<?>> classes = AnnotationUtils.getAnnotatedClasses(
        Algorithm.class, "org.apache.giraph");
    System.out.print("  Supported algorithms:\n");
    for (Class<?> clazz : classes) {
      if (Vertex.class.isAssignableFrom(clazz)) {
        Algorithm algorithm = clazz.getAnnotation(Algorithm.class);
        StringBuilder sb = new StringBuilder();
        sb.append(algorithm.name()).append(" - ").append(clazz.getName())
            .append("\n");
        if (!algorithm.description().equals("")) {
          sb.append("    ").append(algorithm.description()).append("\n");
        }
        System.out.print(sb.toString());
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = getOptions();

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    if (args.length == 0 || cmd.hasOption("h")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    if (cmd.hasOption("la")) {
      printSupportedAlgorithms();
      return 0;
    }

    String vertexClassName = args[0];
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to run Vertex: " + vertexClassName);
    }

    // Verify all the required options have been provided
    if (!cmd.hasOption("w")) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Need to choose the number of workers (-w)");
      }
      return -1;
    }
    if (!cmd.hasOption("vif") && !cmd.hasOption("eif")) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Need to set an input format (-vif or -eif)");
      }
      return -1;
    }

    int workers = Integer.parseInt(cmd.getOptionValue('w'));

    GiraphConfiguration giraphConfiguration = new GiraphConfiguration(
            getConf());

    giraphConfiguration.setVertexClass(
        (Class<? extends Vertex>) Class.forName(vertexClassName));

    GiraphJob job = new GiraphJob(
        giraphConfiguration, "Giraph: " + vertexClassName);

    if (cmd.hasOption("vif")) {
      giraphConfiguration.setVertexInputFormatClass(
          (Class<? extends VertexInputFormat>)
              Class.forName(cmd.getOptionValue("vif")));
      if (cmd.hasOption("vip")) {
        GiraphFileInputFormat.addVertexInputPath(job.getInternalJob(),
            new Path(cmd.getOptionValue("vip")));
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("No vertex input path specified. Ensure your " +
              "VertexInputFormat does not require one.");
        }
      }
    }

    if (cmd.hasOption("eif")) {
      giraphConfiguration.setEdgeInputFormatClass(
          (Class<? extends EdgeInputFormat>)
              Class.forName(cmd.getOptionValue("eif")));
      if (cmd.hasOption("eip")) {
        GiraphFileInputFormat.addEdgeInputPath(job.getInternalJob(),
            new Path(cmd.getOptionValue("eip")));
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("No edge input path specified. Ensure your " +
              "VertexInputFormat does not require one.");
        }
      }
    }

    if (cmd.hasOption("of")) {
      giraphConfiguration.setVertexOutputFormatClass(
          (Class<? extends VertexOutputFormat>)
              Class.forName(cmd.getOptionValue("of")));
      if (cmd.hasOption("op")) {
        FileOutputFormat.setOutputPath(job.getInternalJob(),
            new Path(cmd.getOptionValue("op")));
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("No output path specified. Ensure your VertexOutputFormat " +
              "does not require one.");
        }
      }
    }

    if (cmd.hasOption("c")) {
      giraphConfiguration.setVertexCombinerClass(
          (Class<? extends Combiner>)
              Class.forName(cmd.getOptionValue("c")));
    }

    if (cmd.hasOption("wc")) {
      giraphConfiguration.setWorkerContextClass(
          (Class<? extends WorkerContext>)
              Class.forName(cmd.getOptionValue("wc")));
    }

    if (cmd.hasOption("mc")) {
      giraphConfiguration.setMasterComputeClass(
          (Class<? extends MasterCompute>)
              Class.forName(cmd.getOptionValue("mc")));
    }

    if (cmd.hasOption("aw")) {
      giraphConfiguration.setAggregatorWriterClass(
          (Class<? extends AggregatorWriter>)
              Class.forName(cmd.getOptionValue("aw")));
    }

    if (cmd.hasOption("cf")) {
      DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
          job.getConfiguration());
    }

    if (cmd.hasOption("ca")) {
      Configuration jobConf = job.getConfiguration();
      for (String caOptionValue : cmd.getOptionValues("ca")) {
        for (String paramValue :
            Splitter.on(',').split(caOptionValue)) {
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
    }

    // validate generic parameters chosen are correct or
    // throw IllegalArgumentException, halting execution.
    @SuppressWarnings("rawtypes")
    GiraphTypeValidator<?, ?, ?, ?> validator =
      new GiraphTypeValidator(job.getConfiguration());
    validator.validateClassTypes();

    giraphConfiguration.setWorkerConfiguration(workers, workers, 100.0f);

    boolean verbose = !cmd.hasOption('q');
    return job.run(verbose) ? 0 : -1;
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
