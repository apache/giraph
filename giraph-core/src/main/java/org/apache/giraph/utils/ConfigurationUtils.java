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
package org.apache.giraph.utils;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphConfigurationValidator;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * Translate command line args into Configuration Key-Value pairs.
 */
public final class ConfigurationUtils {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(ConfigurationUtils.class);
  /** The base path for output dirs as saved in GiraphConfiguration */
  private static final Path BASE_OUTPUT_PATH;
  static {
    // whether local or remote, if there's no *-site.xml's to find, we're done
    try {
      BASE_OUTPUT_PATH = FileSystem.get(new Configuration()).getHomeDirectory();
    } catch (IOException ioe) {
      throw new IllegalStateException("Error locating default base path!", ioe);
    }
  }
  /** Maintains our accepted options in case the caller wants to add some */
  private static Options OPTIONS;

  static {
    OPTIONS = new Options();
    OPTIONS.addOption("h", "help", false, "Help");
    OPTIONS.addOption("la", "listAlgorithms", false, "List supported " +
        "algorithms");
    OPTIONS.addOption("q", "quiet", false, "Quiet output");
    OPTIONS.addOption("yj", "yarnjars", true, "comma-separated list of JAR " +
      "filenames to distribute to Giraph tasks and ApplicationMaster. " +
      "YARN only. Search order: CLASSPATH, HADOOP_HOME, user current dir.");
    OPTIONS.addOption("yh", "yarnheap", true, "Heap size, in MB, for each " +
      "Giraph task (YARN only.) Defaults to " +
      GiraphConstants.GIRAPH_YARN_TASK_HEAP_MB + " MB.");
    OPTIONS.addOption("w", "workers", true, "Number of workers");
    OPTIONS.addOption("vif", "vertexInputFormat", true, "Vertex input format");
    OPTIONS.addOption("eif", "edgeInputFormat", true, "Edge input format");
    OPTIONS.addOption("of", "outputFormat", true, "Vertex output format");
    OPTIONS.addOption("vip", "vertexInputPath", true, "Vertex input path");
    OPTIONS.addOption("eip", "edgeInputPath", true, "Edge input path");
    OPTIONS.addOption("op", "outputPath", true, "Vertex output path");
    OPTIONS.addOption("c", "combiner", true, "Combiner class");
    OPTIONS.addOption("ve", "outEdges", true, "Vertex edges class");
    OPTIONS.addOption("wc", "workerContext", true, "WorkerContext class");
    OPTIONS.addOption("aw", "aggregatorWriter", true, "AggregatorWriter class");
    OPTIONS.addOption("mc", "masterCompute", true, "MasterCompute class");
    OPTIONS.addOption("cf", "cacheFile", true, "Files for distributed cache");
    OPTIONS.addOption("pc", "partitionClass", true, "Partition class");
    OPTIONS.addOption("vvf", "vertexValueFactoryClass", true,
        "Vertex value factory class");
    OPTIONS.addOption("ca", "customArguments", true, "provide custom" +
        " arguments for the job configuration in the form:" +
        " -ca <param1>=<value1>,<param2>=<value2> -ca <param3>=<value3> etc." +
        " It can appear multiple times, and the last one has effect" +
        " for the same param.");
  }

  /**
   * No constructing this utility class
   */
  private ConfigurationUtils() { }

  /**
   * Translate CLI arguments to GiraphRunner or 'bin/hadoop jar' into
   * Configuration Key-Value pairs.
   * @param giraphConf the current job Configuration.
   * @param args the raw CLI args to parse
   * @return a CommandLine object, or null if the job run should exit.
   */
  public static CommandLine parseArgs(final GiraphConfiguration giraphConf,
    final String[] args) throws ClassNotFoundException, ParseException,
    IOException {
    // verify we have args at all (can't run without them!)
    if (args.length == 0) {
      throw new IllegalArgumentException("No arguments were provided (try -h)");
    }
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(OPTIONS, args);

    // simply printing help or info, return normally but kill job run
    if (cmd.hasOption("h")) {
      printHelp();
      return null;
    }
    if (cmd.hasOption("la")) {
      printSupportedAlgorithms();
      return null;
    }

    // Be certain that there are no critical args missing, die if so.
    performSanityCheck(cmd);

    // Args are OK; attempt to populate the GiraphConfiguration with them.
    final String vertexClassName = args[0];
    final int workers = Integer.parseInt(cmd.getOptionValue('w'));
    populateGiraphConfiguration(giraphConf, cmd, vertexClassName, workers);

    // validate generic parameters chosen are correct or
    // throw IllegalArgumentException, halting execution.
    @SuppressWarnings("rawtypes")
    GiraphConfigurationValidator<?, ?, ?, ?> gtv =
      new GiraphConfigurationValidator(giraphConf);
    gtv.validateConfiguration();

    // successfully populated/validated GiraphConfiguration, ready to run job
    return cmd;
  }

  /**
   * Callers can place additional options to be parsed and stored in our job's
   * GiraphConfiguration via this utility call. These options will ONLY be
   * parsed and placed into the CommandLine returned from <code>parseArgs</code>
   * Calling code must query this CommandLine to take action on these options.
   * @param opt short options name, i.e. -h
   * @param longOpt long option name, i.e. --help
   * @param hasArg should we expect an argument for this option?
   * @param description English description of this option.
   */
  public static void addOption(final String opt, final String longOpt,
    final boolean hasArg, final String description) {
    if (OPTIONS.hasOption(opt)) {
      printHelp();
      throw new IllegalArgumentException("GiraphConfiguration already " +
        "provides a '" + opt + "' option, please choose another identifier.");
    }
    OPTIONS.addOption(opt, longOpt, hasArg, description);
  }

  /**
   * Utility to check mission-critical args are populated. The validity of
   * the values provided in these args is checked elsewhere.
   * @param cmd our parsed CommandLine
   */
  private static void performSanityCheck(final CommandLine cmd) {
    // Verify all the required options have been provided
    if (!cmd.hasOption("w")) {
      throw new IllegalArgumentException("Need to choose the " +
        "number of workers (-w)");
    }
    if (!cmd.hasOption("vif") && !cmd.hasOption("eif")) {
      throw new IllegalArgumentException("Need to set an input " +
        "format (-vif or -eif)");
    }
  }

  /**
   * Populate GiraphConfiguration for this job with all cmd line args found.
   * Any global configuration data that Giraph on any platform might need
   * should be captured here.
   * @param giraphConfiguration config for this job run
   * @param cmd parsed command line options to store in giraphConfiguration
   * @param vertexClassName the vertex class (application) to run in this job.
   * @param workers the number of worker tasks for this job run.
   */
  private static void populateGiraphConfiguration(final GiraphConfiguration
    giraphConfiguration, final CommandLine cmd, final String vertexClassName,
    final int workers) throws ClassNotFoundException, IOException {
    giraphConfiguration.setWorkerConfiguration(workers, workers, 100.0f);
    giraphConfiguration.setVertexClass(
        (Class<? extends Vertex>) Class.forName(vertexClassName));
    if (cmd.hasOption("c")) {
      giraphConfiguration.setCombinerClass(
          (Class<? extends Combiner>) Class.forName(cmd.getOptionValue("c")));
    }
    if (cmd.hasOption("ve")) {
      giraphConfiguration.setOutEdgesClass(
          (Class<? extends OutEdges>)
              Class.forName(cmd.getOptionValue("ve")));
    }
    if (cmd.hasOption("ive")) {
      giraphConfiguration.setInputOutEdgesClass(
          (Class<? extends OutEdges>)
              Class.forName(cmd.getOptionValue("ive")));
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
    if (cmd.hasOption("vif")) {
      giraphConfiguration.setVertexInputFormatClass(
          (Class<? extends VertexInputFormat>)
              Class.forName(cmd.getOptionValue("vif")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No vertex input format specified. Ensure your " +
          "InputFormat does not require one.");
      }
    }
    if (cmd.hasOption("eif")) {
      giraphConfiguration.setEdgeInputFormatClass(
          (Class<? extends EdgeInputFormat>)
              Class.forName(cmd.getOptionValue("eif")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No edge input format specified. Ensure your " +
          "InputFormat does not require one.");
      }
    }
    if (cmd.hasOption("of")) {
      giraphConfiguration.setVertexOutputFormatClass(
          (Class<? extends VertexOutputFormat>)
              Class.forName(cmd.getOptionValue("of")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No output format specified. Ensure your OutputFormat " +
          "does not require one.");
      }
    }
    if (cmd.hasOption("pc")) {
      giraphConfiguration.setPartitionClass(
          (Class<? extends Partition>)
              Class.forName(cmd.getOptionValue("pc")));
    }
    if (cmd.hasOption("vvf")) {
      giraphConfiguration.setVertexValueFactoryClass(
          (Class<? extends VertexValueFactory>)
              Class.forName(cmd.getOptionValue("vvf")));
    }
    if (cmd.hasOption("ca")) {
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
                parts[1] + "] in GiraphConfiguration");
          }
          giraphConfiguration.set(parts[0], parts[1]);
        }
      }
    }
    // Now, we parse options that are specific to Hadoop MR Job
    if (cmd.hasOption("vif")) {
      if (cmd.hasOption("vip")) {
        GiraphFileInputFormat.addVertexInputPath(giraphConfiguration,
          new Path(cmd.getOptionValue("vip")));
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("No input path for vertex data was specified. Ensure your " +
            "InputFormat does not require one.");
        }
      }
    }
    if (cmd.hasOption("eif")) {
      if (cmd.hasOption("eip")) {
        GiraphFileInputFormat.addEdgeInputPath(giraphConfiguration,
          new Path(cmd.getOptionValue("eip")));
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("No input path for edge data was specified. Ensure your " +
            "InputFormat does not require one.");
        }
      }
    }
    // YARN-ONLY OPTIONS
    if (cmd.hasOption("yj")) {
      giraphConfiguration.setYarnLibJars(cmd.getOptionValue("yj"));
    }
    if (cmd.hasOption("yh")) {
      giraphConfiguration.setYarnTaskHeapMb(
        Integer.parseInt(cmd.getOptionValue("yh")));
    }
    /*if[PURE_YARN]
    if (cmd.hasOption("of")) {
      if (cmd.hasOption("op")) {
        // For YARN conf to get the out dir we need w/o a Job obj
        Path outputDir =
            new Path(BASE_OUTPUT_PATH, cmd.getOptionValue("op"));
        outputDir =
          outputDir.getFileSystem(giraphConfiguration).makeQualified(outputDir);
        giraphConfiguration.set(
            org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR,
            outputDir.toString());

      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info("No output path specified. Ensure your OutputFormat " +
            "does not require one.");
        }
      }
    }
    end[PURE_YARN]*/
    // END YARN-ONLY OPTIONS
  }

  /**
   * Utility to print CLI help messsage for registered options.
   */
  private static void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(ConfigurationUtils.class.getName(), OPTIONS, true);
  }

  /**
   * Prints description of algorithms annotated with
   * {@link org.apache.giraph.Algorithm}
   */
  private static void printSupportedAlgorithms() {
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
}
