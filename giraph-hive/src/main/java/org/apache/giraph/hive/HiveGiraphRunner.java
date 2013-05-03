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

package org.apache.giraph.hive;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.giraph.conf.GiraphClasses;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.input.vertex.HiveVertexInputFormat;
import org.apache.giraph.hive.output.HiveVertexOutputFormat;
import org.apache.giraph.hive.output.VertexToHive;
import org.apache.giraph.io.formats.multi.EdgeInputFormatDescription;
import org.apache.giraph.io.formats.multi.InputFormatDescription;
import org.apache.giraph.io.formats.multi.MultiEdgeInputFormat;
import org.apache.giraph.io.formats.multi.MultiVertexInputFormat;
import org.apache.giraph.io.formats.multi.VertexInputFormatDescription;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT_DATABASE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT_PARTITION;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT_TABLE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_EDGE_CLASS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_VERTEX_CLASS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT_DATABASE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT_PARTITION;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT_TABLE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_DATABASE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PARTITION;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_TABLE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.VERTEX_TO_HIVE_CLASS;

/**
 * Hive Giraph Runner
 */
public class HiveGiraphRunner implements Tool {
  /** logger */
  private static final Logger LOG = Logger.getLogger(HiveGiraphRunner.class);
  /** Prefix for log statements */
  private static final String LOG_PREFIX = "\t";

  /** workers */
  protected int workers;
  /** is verbose */
  protected boolean isVerbose;

  /** vertex class. */
  private Class<? extends Vertex> vertexClass;

  /** Descriptions of vertex input formats */
  private List<VertexInputFormatDescription> vertexInputDescriptions =
      Lists.newArrayList();

  /** Descriptions of edge input formats */
  private List<EdgeInputFormatDescription> edgeInputDescriptions =
      Lists.newArrayList();

  /** Hive Vertex writer */
  private Class<? extends VertexToHive> vertexToHiveClass;
  /** Skip output? (Useful for testing without writing) */
  private boolean skipOutput = false;

  /** Configuration */
  private Configuration conf;

  /** Create a new runner */
  public HiveGiraphRunner() {
    conf = new HiveConf(getClass());
  }

  public Class<? extends Vertex> getVertexClass() {
    return vertexClass;
  }

  public void setVertexClass(Class<? extends Vertex> vertexClass) {
    this.vertexClass = vertexClass;
  }

  public List<VertexInputFormatDescription> getVertexInputDescriptions() {
    return vertexInputDescriptions;
  }

  /**
   * Whether to use vertex input.
   *
   * @return true if vertex input enabled (at least one HiveToVertex is set).
   */
  public boolean hasVertexInput() {
    return !vertexInputDescriptions.isEmpty();
  }

  /**
   * Add vertex input
   *
   * @param hiveToVertexClass HiveToVertex class to use
   * @param tableName Table name
   * @param partitionFilter Partition filter, or null if no filter used
   * @param additionalOptions Additional options, in the form "option=value"
   */
  public void addVertexInput(Class<? extends HiveToVertex> hiveToVertexClass,
      String tableName, String partitionFilter, String ... additionalOptions) {
    VertexInputFormatDescription description =
        new VertexInputFormatDescription(HiveVertexInputFormat.class);
    description.addParameter(
        HIVE_TO_VERTEX_CLASS.getKey(), hiveToVertexClass.getName());
    description.addParameter(HIVE_VERTEX_INPUT_PROFILE_ID.getKey(),
        "vertex_input_profile_" + vertexInputDescriptions.size());
    description.addParameter(
        HIVE_VERTEX_INPUT_TABLE.getKey(), tableName);
    if (partitionFilter != null && !partitionFilter.isEmpty()) {
      description.addParameter(
          HIVE_VERTEX_INPUT_PARTITION.getKey(), partitionFilter);
    }
    addAdditionalOptions(description, additionalOptions);
    vertexInputDescriptions.add(description);
  }

  public List<EdgeInputFormatDescription> getEdgeInputDescriptions() {
    return edgeInputDescriptions;
  }

  /**
   * Whether to use edge input.
   *
   * @return true if edge input enabled (at least one HiveToEdge is set).
   */
  public boolean hasEdgeInput() {
    return !edgeInputDescriptions.isEmpty();
  }

  /**
   * Add edge input
   *
   * @param hiveToEdgeClass HiveToEdge class to use
   * @param tableName Table name
   * @param partitionFilter Partition filter, or null if no filter used
   * @param additionalOptions Additional options, in the form "option=value"
   */
  public void addEdgeInput(Class<? extends HiveToEdge> hiveToEdgeClass,
      String tableName, String partitionFilter, String ... additionalOptions) {
    EdgeInputFormatDescription description =
        new EdgeInputFormatDescription(HiveEdgeInputFormat.class);
    description.addParameter(
        HIVE_TO_EDGE_CLASS.getKey(), hiveToEdgeClass.getName());
    description.addParameter(HIVE_EDGE_INPUT_PROFILE_ID.getKey(),
        "edge_input_profile_" + edgeInputDescriptions.size());
    description.addParameter(
        HIVE_EDGE_INPUT_TABLE.getKey(), tableName);
    if (partitionFilter != null && !partitionFilter.isEmpty()) {
      description.addParameter(
          HIVE_EDGE_INPUT_PARTITION.getKey(), partitionFilter);
    }
    addAdditionalOptions(description, additionalOptions);
    edgeInputDescriptions.add(description);
  }

  /**
   * Add additional options to InputFormatDescription
   *
   * @param description InputFormatDescription
   * @param additionalOptions Additional options
   */
  private static void addAdditionalOptions(InputFormatDescription description,
      String ... additionalOptions) {
    for (String additionalOption : additionalOptions) {
      String[] nameValue = split(additionalOption, "=");
      if (nameValue.length != 2) {
        throw new IllegalStateException("Invalid additional option format " +
            additionalOption + ", 'name=value' format expected");
      }
      description.addParameter(nameValue[0], nameValue[1]);
    }
  }

  public Class<? extends VertexToHive> getVertexToHiveClass() {
    return vertexToHiveClass;
  }

  /**
   * Whether we are writing vertices out.
   *
   * @return true if vertex output enabled
   */
  public boolean hasVertexOutput() {
    return !skipOutput && vertexToHiveClass != null;
  }

  /**
   * Set vertex output
   *
   * @param vertexToHiveClass class for writing vertices to Hive.
   * @param tableName Table name
   * @param partitionFilter Partition filter, or null if no filter used
   */
  public void setVertexOutput(
      Class<? extends VertexToHive> vertexToHiveClass, String tableName,
      String partitionFilter) {
    this.vertexToHiveClass = vertexToHiveClass;
    VERTEX_TO_HIVE_CLASS.set(conf, vertexToHiveClass);
    HIVE_VERTEX_OUTPUT_PROFILE_ID.set(conf, "vertex_output_profile");
    HIVE_VERTEX_OUTPUT_TABLE.set(conf, tableName);
    if (partitionFilter != null) {
      HIVE_VERTEX_OUTPUT_PARTITION.set(conf, partitionFilter);
    }
  }

  /**
   * main method
   * @param args system arguments
   * @throws Exception any errors from Hive Giraph Runner
   */
  public static void main(String[] args) throws Exception {
    HiveGiraphRunner runner = new HiveGiraphRunner();
    System.exit(ToolRunner.run(runner, args));
  }

  @Override
  public final int run(String[] args) throws Exception {
    // process args
    try {
      handleCommandLine(args);
    } catch (InterruptedException e) {
      return 0;
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      return -1;
    }

    // additional configuration for Hive
    adjustConfigurationForHive();

    // setup GiraphJob
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    GiraphConfiguration giraphConf = job.getConfiguration();
    giraphConf.setVertexClass(vertexClass);

    giraphConf.setWorkerConfiguration(workers, workers, 100.0f);
    initGiraphJob(job);

    logOptions(giraphConf);

    return job.run(isVerbose) ? 0 : -1;
  }

  /**
   * Prepare vertex input settings in Configuration
   */
  @SuppressWarnings("unchecked")
  public void prepareHiveVertexInputs() {
    if (vertexInputDescriptions.size() == 1) {
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
          vertexInputDescriptions.get(0).getInputFormatClass());
      vertexInputDescriptions.get(0).putParametersToConfiguration(conf);
    } else {
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
          MultiVertexInputFormat.class);
      VertexInputFormatDescription.VERTEX_INPUT_FORMAT_DESCRIPTIONS.set(conf,
          InputFormatDescription.toJsonString(vertexInputDescriptions));
    }
  }

  /**
   * Prepare edge input settings in Configuration
   */
  @SuppressWarnings("unchecked")
  public void prepareHiveEdgeInputs() {
    if (edgeInputDescriptions.size() == 1) {
      GiraphConstants.EDGE_INPUT_FORMAT_CLASS.set(conf,
          edgeInputDescriptions.get(0).getInputFormatClass());
      edgeInputDescriptions.get(0).putParametersToConfiguration(conf);
    } else {
      GiraphConstants.EDGE_INPUT_FORMAT_CLASS.set(conf,
          MultiEdgeInputFormat.class);
      EdgeInputFormatDescription.EDGE_INPUT_FORMAT_DESCRIPTIONS.set(conf,
          InputFormatDescription.toJsonString(edgeInputDescriptions));
    }
  }

  /**
   * Prepare output settings in Configuration
   */
  public void prepareHiveOutput() {
    GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS.set(conf,
        HiveVertexOutputFormat.class);
  }

  /**
   * set hive configuration
   */
  private void adjustConfigurationForHive() {
    // when output partitions are used, workers register them to the
    // metastore at cleanup stage, and on HiveConf's initialization, it
    // looks for hive-site.xml.
    addToStringCollection(conf, "tmpfiles", conf.getClassLoader()
        .getResource("hive-site.xml").toString());

    // Or, more effectively, we can provide all the jars client needed to
    // the workers as well
    String[] hadoopJars = System.getenv("HADOOP_CLASSPATH").split(
        File.pathSeparator);
    List<String> hadoopJarURLs = Lists.newArrayList();
    for (String jarPath : hadoopJars) {
      File file = new File(jarPath);
      if (file.exists() && file.isFile()) {
        String jarURL = file.toURI().toString();
        hadoopJarURLs.add(jarURL);
      }
    }
    addToStringCollection(conf, "tmpjars", hadoopJarURLs);
  }

  /**
   * process arguments
   * @param args to process
   * @return CommandLine instance
   * @throws org.apache.commons.cli.ParseException error parsing arguments
   * @throws InterruptedException interrupted
   */
  private CommandLine handleCommandLine(String[] args) throws ParseException,
      InterruptedException {
    Options options = new Options();
    addOptions(options);
    addMoreOptions(options);

    CommandLineParser parser = new GnuParser();
    final CommandLine cmdln = parser.parse(options, args);
    if (args.length == 0 || cmdln.hasOption("help")) {
      new HelpFormatter().printHelp(getClass().getName(), options, true);
      throw new InterruptedException();
    }

    // Giraph classes
    String vertexClassStr = cmdln.getOptionValue("vertexClass");
    if (vertexClassStr != null) {
      vertexClass = findClass(vertexClassStr, Vertex.class);
    }
    if (vertexClass == null) {
      throw new IllegalArgumentException(
          "Need the Giraph " + Vertex.class.getSimpleName() +
              " class name (-vertexClass) to use");
    }

    String[] vertexInputs = cmdln.getOptionValues("vertexInput");
    if (vertexInputs != null && vertexInputs.length != 0) {
      vertexInputDescriptions.clear();
      for (String vertexInput : vertexInputs) {
        String[] parameters = split(vertexInput, ",");
        if (parameters.length < 2) {
          throw new IllegalStateException("Illegal vertex input description " +
              vertexInput + " - HiveToVertex class and table name needed");
        }
        addVertexInput(findClass(parameters[0], HiveToVertex.class),
            parameters[1], elementOrNull(parameters, 2),
            copyOfArray(parameters, 3));
      }
    }

    String[] edgeInputs = cmdln.getOptionValues("edgeInput");
    if (edgeInputs != null && edgeInputs.length != 0) {
      edgeInputDescriptions.clear();
      for (String edgeInput : edgeInputs) {
        String[] parameters = split(edgeInput, ",");
        if (parameters.length < 2) {
          throw new IllegalStateException("Illegal edge input description " +
              edgeInput + " - HiveToEdge class and table name needed");
        }
        addEdgeInput(findClass(parameters[0], HiveToEdge.class),
            parameters[1], elementOrNull(parameters, 2),
            copyOfArray(parameters, 3));
      }
    }

    String output = cmdln.getOptionValue("output");
    if (output != null) {
      // Partition filter can contain commas so we limit the number of times
      // we split
      String[] parameters = split(output, ",", 3);
      if (parameters.length < 2) {
        throw new IllegalStateException("Illegal output description " +
            output + " - VertexToHive class and table name needed");
      }
      setVertexOutput(findClass(parameters[0], VertexToHive.class),
          parameters[1], elementOrNull(parameters, 2));
    }

    if (cmdln.hasOption("skipOutput")) {
      skipOutput = true;
    }

    if (!hasVertexInput() && !hasEdgeInput()) {
      throw new IllegalArgumentException(
          "Need at least one of Giraph " +
          HiveToVertex.class.getSimpleName() +
          " (-vertexInput) and " +
          HiveToEdge.class.getSimpleName() +
          " (-edgeInput)");
    }
    if (vertexToHiveClass == null && !skipOutput) {
      throw new IllegalArgumentException(
          "Need the Giraph " + VertexToHive.class.getSimpleName() +
          " (-output) to use");
    }
    String workersStr = cmdln.getOptionValue("workers");
    if (workersStr == null) {
      throw new IllegalArgumentException(
          "Need to choose the number of workers (-w)");
    }

    String dbName = cmdln.getOptionValue("dbName", "default");

    if (hasVertexInput()) {
      HIVE_VERTEX_INPUT_DATABASE.set(conf, dbName);
      prepareHiveVertexInputs();
    }

    if (hasEdgeInput()) {
      HIVE_EDGE_INPUT_DATABASE.set(conf, dbName);
      prepareHiveEdgeInputs();
    }

    if (!skipOutput) {
      HIVE_VERTEX_OUTPUT_DATABASE.set(conf, dbName);
      prepareHiveOutput();
    } else {
      LOG.warn("run: Warning - Output will be skipped!");
    }

    workers = Integer.parseInt(workersStr);

    isVerbose = cmdln.hasOption("verbose");

    // pick up -hiveconf arguments
    processHiveConfOptions(cmdln);

    processMoreArguments(cmdln);

    return cmdln;
  }

  /**
   * Process -hiveconf options from command line
   *
   * @param cmdln Command line options
   */
  private void processHiveConfOptions(CommandLine cmdln) {
    for (String hiveconf : cmdln.getOptionValues("hiveconf")) {
      String[] keyval = hiveconf.split("=", 2);
      if (keyval.length == 2) {
        String name = keyval[0];
        String value = keyval[1];
        if (name.equals("tmpjars") || name.equals("tmpfiles")) {
          addToStringCollection(conf, name, value);
        } else {
          conf.set(name, value);
        }
      }
    }
  }

  /**
   * Add hive-related options to command line parser options
   *
   * @param options Options to use
   */
  private void addOptions(Options options) {
    options.addOption("h", "help", false, "Help");
    options.addOption("v", "verbose", false, "Verbose");
    options.addOption("D", "hiveconf", true,
                "property=value for Hive/Hadoop configuration");
    options.addOption("w", "workers", true, "Number of workers");

    if (vertexClass == null) {
      options.addOption(null, "vertexClass", true,
          "Giraph Vertex class to use");
    }

    options.addOption("db", "dbName", true, "Hive database name");

    // Vertex input settings
    options.addOption("vi", "vertexInput", true, getInputOptionDescription(
        "vertex", HiveToVertex.class.getSimpleName()));

    // Edge input settings
    options.addOption("ei", "edgeInput", true, getInputOptionDescription(
        "edge", HiveToEdge.class.getSimpleName()));

    // Vertex output settings
    options.addOption("o", "output", true,
        "Giraph " + VertexToHive.class.getSimpleName() + " class to use," +
            " table name and partition filter (optional). Example:\n" +
            "\"MyVertexToHive, myTableName, a=1,b=two\"");
    options.addOption("s", "skipOutput", false, "Skip output?");
  }

  /**
   * Get description for the input format option (vertex or edge).
   *
   * @param inputType Type of input (vertex or edge)
   * @param hiveToObjectClassName HiveToVertex or HiveToEdge
   * @return Description for the input format option
   */
  private static String getInputOptionDescription(String inputType,
      String hiveToObjectClassName) {
    StringBuilder inputOption = new StringBuilder();
    inputOption.append("Giraph ").append(hiveToObjectClassName).append(
        " class to use, table name and partition filter (optional).");
    inputOption.append(" Additional options for the input format can be " +
        "specified as well.");
    inputOption.append(" You can set as many ").append(inputType).append(
        " inputs as you like.");
    inputOption.append(" Example:\n");
    inputOption.append("\"My").append(hiveToObjectClassName).append(
        ", myTableName, a<2 AND b='two', option1=value1, option2=value2\"");
    return inputOption.toString();
  }

  /**
   * add string to collection
   * @param conf Configuration
   * @param name name to add
   * @param values values for collection
   */
  private static void addToStringCollection(Configuration conf, String name,
      String... values) {
    addToStringCollection(conf, name, Arrays.asList(values));
  }

  /**
   * add string to collection
   * @param conf Configuration
   * @param name to add
   * @param values values for collection
   */
  private static void addToStringCollection(
      Configuration conf, String name, Collection
      <? extends String> values) {
    Collection<String> tmpfiles = conf.getStringCollection(name);
    tmpfiles.addAll(values);
    conf.setStrings(name, tmpfiles.toArray(new String[tmpfiles.size()]));
  }

  /**
   *
   * @param className to find
   * @param base  base class
   * @param <T> class type found
   * @return type found
   */
  private <T> Class<? extends T> findClass(String className, Class<T> base) {
    try {
      Class<?> cls = Class.forName(className);
      if (base.isAssignableFrom(cls)) {
        return cls.asSubclass(base);
      }
      return null;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(className + ": Invalid class name");
    }
  }

  @Override
  public final Configuration getConf() {
    return conf;
  }

  @Override
  public final void setConf(Configuration conf) {
    this.conf = new GiraphConfiguration(conf);
  }

  /**
   * Override this method to add more command-line options. You can process
   * them by also overriding {@link #processMoreArguments(CommandLine)}.
   *
   * @param options Options
   */
  protected void addMoreOptions(Options options) {
  }

  /**
   * Override this method to process additional command-line arguments. You
   * may want to declare additional options by also overriding
   * {@link #addMoreOptions(org.apache.commons.cli.Options)}.
   *
   * @param cmd Command
   */
  protected void processMoreArguments(CommandLine cmd) {
  }

  /**
   * Override this method to do additional setup with the GiraphJob that will
   * run.
   *
   * @param job GiraphJob that is going to run
   */
  protected void initGiraphJob(GiraphJob job) { }

  /**
   * Log the options set by user
   *
   * @param giraphConf GiraphConfiguration
   */
  private void logOptions(GiraphConfiguration giraphConf) {
    GiraphClasses<?, ?, ?, ?> classes = new GiraphClasses(giraphConf);

    LOG.info(getClass().getSimpleName() + " with");

    LOG.info(LOG_PREFIX + "-vertexClass=" + vertexClass.getCanonicalName());

    for (VertexInputFormatDescription description : vertexInputDescriptions) {
      LOG.info(LOG_PREFIX + "Vertex input: " + description);
    }

    for (EdgeInputFormatDescription description : edgeInputDescriptions) {
      LOG.info(LOG_PREFIX + "Edge input: " + description);
    }

    if (classes.getVertexOutputFormatClass() != null) {
      LOG.info(LOG_PREFIX + "Output: VertexToHive=" +
          vertexToHiveClass.getCanonicalName() + ", table=" +
          HIVE_VERTEX_OUTPUT_TABLE.get(conf) + ", partition=\"" +
          HIVE_VERTEX_OUTPUT_PARTITION.get(conf) + "\"");
    }

    LOG.info(LOG_PREFIX + "-workers=" + workers);
  }

  /**
   * Split a string using separator and trim the results
   *
   * @param stringToSplit String to split
   * @param separator Separator
   * @return Separated strings, trimmed
   */
  private static String[] split(String stringToSplit, String separator) {
    return split(stringToSplit, separator, -1);
  }

  /**
   * Split a string using separator and trim the results
   *
   * @param stringToSplit String to split
   * @param separator Separator
   * @param limit See {@link String#split(String, int)}
   * @return Separated strings, trimmed
   */
  private static String[] split(String stringToSplit, String separator,
      int limit) {
    Splitter splitter = Splitter.on(separator).trimResults();
    if (limit > 0) {
      splitter = splitter.limit(limit);
    }
    return Iterables.toArray(splitter.split(stringToSplit), String.class);
  }

  /**
   * Get the element in array at certain position, or null if the position is
   * out of array size
   *
   * @param array Array
   * @param position Position
   * @return Element at the position or null if the position is out of array
   */
  private static String elementOrNull(String[] array, int position) {
    return (position < array.length) ? array[position] : null;
  }

  /**
   * Return a copy of array from some position to the end,
   * or empty array if startIndex is out of array size
   *
   * @param array Array to take a copy from
   * @param startIndex Starting position
   * @return Copy of part of the array
   */
  private static String[] copyOfArray(String[] array, int startIndex) {
    if (array.length <= startIndex) {
      return new String[0];
    } else {
      return Arrays.copyOfRange(array, startIndex, array.length);
    }
  }
}
