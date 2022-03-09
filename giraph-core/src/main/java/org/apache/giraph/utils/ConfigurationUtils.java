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

import static org.apache.giraph.conf.GiraphConstants.COMPUTATION_CLASS;
import static org.apache.giraph.conf.GiraphConstants.TYPES_HOLDER_CLASS;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.GiraphTypes;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.factories.VertexValueFactory;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.Language;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphConfigurationValidator;
import org.apache.giraph.jython.JythonUtils;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.scripting.DeployType;
import org.apache.giraph.scripting.ScriptLoader;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * Translate command line args into Configuration Key-Value pairs.
 */
public final class ConfigurationUtils {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(ConfigurationUtils.class);
/*if[PURE_YARN]
  // The base path for output dirs as saved in GiraphConfiguration
  private static final Path BASE_OUTPUT_PATH;
  static {
    // whether local or remote, if there's no *-site.xml's to find, we're done
    try {
      BASE_OUTPUT_PATH = FileSystem.get(new Configuration()).getHomeDirectory();
    } catch (IOException ioe) {
      throw new IllegalStateException("Error locating default base path!", ioe);
    }
  }
end[PURE_YARN]*/
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
    OPTIONS.addOption("vof", "vertexOutputFormat", true,
        "Vertex output format");
    OPTIONS.addOption("eof", "edgeOutputFormat", true, "Edge output format");
    OPTIONS.addOption("vip", "vertexInputPath", true, "Vertex input path");
    OPTIONS.addOption("eip", "edgeInputPath", true, "Edge input path");
    OPTIONS.addOption("op",  "outputPath", true, "Output path");
    OPTIONS.addOption("vsd",  "vertexSubDir", true, "subdirectory to be used " +
        "for the vertex output");
    OPTIONS.addOption("esd",  "edgeSubDir", true, "subdirectory to be used " +
        "for the edge output");
    OPTIONS.addOption("c", "combiner", true, "MessageCombiner class");
    OPTIONS.addOption("ve", "outEdges", true, "Vertex edges class");
    OPTIONS.addOption("wc", "workerContext", true, "WorkerContext class");
    OPTIONS.addOption("aw", "aggregatorWriter", true, "AggregatorWriter class");
    OPTIONS.addOption("mc", "masterCompute", true, "MasterCompute class");
    OPTIONS.addOption("cf", "cacheFile", true, "Files for distributed cache");
    OPTIONS.addOption("pc", "partitionClass", true, "Partition class");
    OPTIONS.addOption("vvf", "vertexValueFactoryClass", true,
        "Vertex value factory class");
    OPTIONS.addOption("th", "typesHolder", true,
        "Class that holds types. Needed only if Computation is not set");
    OPTIONS.addOption("jyc", "jythonClass", true,
        "Jython class name, used if computation passed in is a python script");
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
   * Configure an object with an
   * {@link org.apache.giraph.conf.ImmutableClassesGiraphConfiguration}
   * if that objects supports it.
   *
   * @param object The object to configure
   * @param configuration The configuration
   */
  public static void configureIfPossible(Object object,
      ImmutableClassesGiraphConfiguration configuration) {
    if (configuration != null) {
      configuration.configureIfPossible(object);
    } else if (object instanceof GiraphConfigurationSettable) {
      throw new IllegalArgumentException(
          "Trying to configure configurable object without value, " +
          object.getClass());
    }
  }

  /**
   * Get a class which is parameterized by the graph types defined by user.
   * The types holder is actually an interface that any class which holds all of
   * Giraph types can implement. It is used with reflection to infer the Giraph
   * types.
   *
   * The current order of type holders we try are:
   * 1) The {@link TypesHolder} class directly.
   * 2) The {@link Computation} class, as that holds all the types.
   *
   * @param conf Configuration
   * @return {@link TypesHolder} or null if could not find one.
   */
  public static Class<? extends TypesHolder> getTypesHolderClass(
      Configuration conf) {
    Class<? extends TypesHolder> klass = TYPES_HOLDER_CLASS.get(conf);
    if (klass != null) {
      return klass;
    }
    klass = COMPUTATION_CLASS.get(conf);
    return klass;
  }

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
    final String computationClassName = args[0];
    final int workers = Integer.parseInt(cmd.getOptionValue('w'));
    populateGiraphConfiguration(giraphConf, cmd, computationClassName, workers);

    // validate generic parameters chosen are correct or
    // throw IllegalArgumentException, halting execution.
    @SuppressWarnings("rawtypes")
    GiraphConfigurationValidator<?, ?, ?, ?, ?> gtv =
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
   *
   * @param conf config for this job run
   * @param cmd parsed command line options to store in conf
   * @param computationClassName the computation class (application) to run in
   *                             this job.
   * @param workers the number of worker tasks for this job run.
   */
  private static void populateGiraphConfiguration(final GiraphConfiguration
    conf, final CommandLine cmd,
    final String computationClassName, final int workers)
    throws ClassNotFoundException, IOException {
    conf.setWorkerConfiguration(workers, workers, 100.0f);
    if (cmd.hasOption("typesHolder")) {
      Class<? extends TypesHolder> typesHolderClass =
          (Class<? extends TypesHolder>)
              Class.forName(cmd.getOptionValue("typesHolder"));
      TYPES_HOLDER_CLASS.set(conf, typesHolderClass);
    }
    if (cmd.hasOption("c")) {
      conf.setMessageCombinerClass(
          (Class<? extends MessageCombiner>)
              Class.forName(cmd.getOptionValue("c")));
    }
    if (cmd.hasOption("vc")) {
      conf.setVertexValueCombinerClass(
          (Class<? extends VertexValueCombiner>)
              Class.forName(cmd.getOptionValue("vc")));
    }
    if (cmd.hasOption("ve")) {
      conf.setOutEdgesClass(
          (Class<? extends OutEdges>) Class.forName(cmd.getOptionValue("ve")));
    }
    if (cmd.hasOption("ive")) {
      conf.setInputOutEdgesClass(
          (Class<? extends OutEdges>) Class.forName(cmd.getOptionValue("ive")));
    }
    if (cmd.hasOption("wc")) {
      conf.setWorkerContextClass(
          (Class<? extends WorkerContext>) Class
              .forName(cmd.getOptionValue("wc")));
    }
    if (cmd.hasOption("mc")) {
      conf.setMasterComputeClass(
          (Class<? extends MasterCompute>) Class
              .forName(cmd.getOptionValue("mc")));
    }
    if (cmd.hasOption("aw")) {
      conf.setAggregatorWriterClass(
          (Class<? extends AggregatorWriter>) Class
              .forName(cmd.getOptionValue("aw")));
    }
    if (cmd.hasOption("vif")) {
      conf.setVertexInputFormatClass(
          (Class<? extends VertexInputFormat>) Class
              .forName(cmd.getOptionValue("vif")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No vertex input format specified. Ensure your " +
          "InputFormat does not require one.");
      }
    }
    if (cmd.hasOption("eif")) {
      conf.setEdgeInputFormatClass(
          (Class<? extends EdgeInputFormat>) Class
              .forName(cmd.getOptionValue("eif")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No edge input format specified. Ensure your " +
          "InputFormat does not require one.");
      }
    }
    if (cmd.hasOption("vof")) {
      conf.setVertexOutputFormatClass(
          (Class<? extends VertexOutputFormat>) Class
              .forName(cmd.getOptionValue("vof")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No vertex output format specified. Ensure your " +
          "OutputFormat does not require one.");
      }
    }
    if (cmd.hasOption("vof")) {
      if (cmd.hasOption("vsd")) {
        conf.setVertexOutputFormatSubdir(cmd.getOptionValue("vsd"));
      }
    }
    if (cmd.hasOption("eof")) {
      conf.setEdgeOutputFormatClass(
          (Class<? extends EdgeOutputFormat>) Class
              .forName(cmd.getOptionValue("eof")));
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("No edge output format specified. Ensure your " +
          "OutputFormat does not require one.");
      }
    }
    if (cmd.hasOption("eof")) {
      if (cmd.hasOption("esd")) {
        conf.setEdgeOutputFormatSubdir(cmd.getOptionValue("esd"));
      }
    }
    /* check for path clashes */
    if (cmd.hasOption("vof") && cmd.hasOption("eof") && cmd.hasOption("op")) {
      if (!cmd.hasOption("vsd") || cmd.hasOption("esd")) {
        if (!conf.hasEdgeOutputFormatSubdir() ||
            !conf.hasVertexOutputFormatSubdir()) {

          throw new IllegalArgumentException("If VertexOutputFormat and " +
              "EdgeOutputFormat are both set, it is mandatory to provide " +
              "both vertex subdirectory as well as edge subdirectory");
        }
      }
    }
    if (cmd.hasOption("pc")) {
      conf.setPartitionClass(
          (Class<? extends Partition>) Class.forName(cmd.getOptionValue("pc")));
    }
    if (cmd.hasOption("vvf")) {
      conf.setVertexValueFactoryClass(
          (Class<? extends VertexValueFactory>) Class
              .forName(cmd.getOptionValue("vvf")));
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
          conf.set(parts[0], parts[1]);
        }
      }
    }
    // Now, we parse options that are specific to Hadoop MR Job
    if (cmd.hasOption("vif")) {
      if (cmd.hasOption("vip")) {
        if (FileSystem.get(new Configuration()).listStatus(
              new Path(cmd.getOptionValue("vip"))) == null) {
          throw new IllegalArgumentException(
              "Invalid vertex input path (-vip): " +
              cmd.getOptionValue("vip"));
        }
        GiraphFileInputFormat.addVertexInputPath(conf,
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
        if (FileSystem.get(new Configuration()).listStatus(
              new Path(cmd.getOptionValue("eip"))) == null) {
          throw new IllegalArgumentException(
              "Invalid edge input path (-eip): " +
              cmd.getOptionValue("eip"));
        }
        GiraphFileInputFormat.addEdgeInputPath(conf,
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
      conf.setYarnLibJars(cmd.getOptionValue("yj"));
    }
    if (cmd.hasOption("yh")) {
      conf.setYarnTaskHeapMb(
          Integer.parseInt(cmd.getOptionValue("yh")));
    }
/*if[PURE_YARN]
    if (cmd.hasOption("vof") || cmd.hasOption("eof")) {
      if (cmd.hasOption("op")) {
        // For YARN conf to get the out dir we need w/o a Job obj
        Path outputDir =
            new Path(BASE_OUTPUT_PATH, cmd.getOptionValue("op"));
        outputDir =
          outputDir.getFileSystem(conf).makeQualified(outputDir);
        conf.set(
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
    handleComputationClass(conf, cmd, computationClassName);
  }

  /**
   * Helper to deal with computation class.
   *
   * @param conf Configuration
   * @param cmd CommandLine
   * @param computationClassName Name of computation
   * @throws ClassNotFoundException error finding class
   */
  private static void handleComputationClass(GiraphConfiguration conf,
    CommandLine cmd, String computationClassName)
    throws ClassNotFoundException {
    if (computationClassName.endsWith("py")) {
      handleJythonComputation(conf, cmd, computationClassName);
    } else {
      conf.setComputationClass(
          (Class<? extends Computation>) Class.forName(computationClassName));
    }
  }

  /**
   * Helper to handle Computations implemented in Python.
   *
   * @param conf Configuration
   * @param cmd CommandLine
   * @param scriptPath Path to python script
   */
  private static void handleJythonComputation(GiraphConfiguration conf,
    CommandLine cmd, String scriptPath) {
    String jythonClass = cmd.getOptionValue("jythonClass");
    if (jythonClass == null) {
      throw new IllegalArgumentException(
          "handleJythonComputation: Need to set Jython Computation class " +
          "name with --jythonClass");
    }
    String typesHolderClass = cmd.getOptionValue("typesHolder");
    if (typesHolderClass == null) {
      throw new IllegalArgumentException(
          "handleJythonComputation: Need to set TypesHolder class name " +
          "with --typesHolder");
    }

    Path path = new Path(scriptPath);
    Path remotePath = DistributedCacheUtils.copyAndAdd(path, conf);

    ScriptLoader.setScriptsToLoad(conf, remotePath.toString(),
        DeployType.DISTRIBUTED_CACHE, Language.JYTHON);

    GiraphTypes.readFrom(conf).writeIfUnset(conf);
    JythonUtils.init(conf, jythonClass);
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
      if (Computation.class.isAssignableFrom(clazz)) {
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
