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
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.mapping.HiveMappingInputFormat;
import org.apache.giraph.hive.input.mapping.HiveToMapping;
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

import java.util.Arrays;
import java.util.List;

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_MAPPING_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT;
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
  private Class<? extends Computation> computationClass;

  /** Descriptions of vertex input formats */
  private List<VertexInputFormatDescription> vertexInputDescriptions =
      Lists.newArrayList();

  /** Descriptions of edge input formats */
  private List<EdgeInputFormatDescription> edgeInputDescriptions =
      Lists.newArrayList();

  /** Hive Mapping reader */
  private Class<? extends HiveToMapping> hiveToMappingClass;
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

  public Class<? extends Computation> getComputationClass() {
    return computationClass;
  }

  public void setComputationClass(
      Class<? extends Computation> computationClass) {
    this.computationClass = computationClass;
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
        HIVE_VERTEX_INPUT.getClassOpt().getKey(), hiveToVertexClass.getName());
    description.addParameter(HIVE_VERTEX_INPUT.getProfileIdOpt().getKey(),
        "vertex_input_profile_" + vertexInputDescriptions.size());
    description.addParameter(
        HIVE_VERTEX_INPUT.getTableOpt().getKey(), tableName);
    if (partitionFilter != null && !partitionFilter.isEmpty()) {
      description.addParameter(
          HIVE_VERTEX_INPUT.getPartitionOpt().getKey(), partitionFilter);
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
        HIVE_EDGE_INPUT.getClassOpt().getKey(), hiveToEdgeClass.getName());
    description.addParameter(HIVE_EDGE_INPUT.getProfileIdOpt().getKey(),
        "edge_input_profile_" + edgeInputDescriptions.size());
    description.addParameter(
        HIVE_EDGE_INPUT.getTableOpt().getKey(), tableName);
    if (partitionFilter != null && !partitionFilter.isEmpty()) {
      description.addParameter(
          HIVE_EDGE_INPUT.getPartitionOpt().getKey(), partitionFilter);
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
   * Check if mapping input is set
   *
   * @return true if mapping input is set
   */
  public boolean hasMappingInput() {
    return hiveToMappingClass != null;
  }

  /**
   * Set mapping input
   *
   * @param hiveToMappingClass class for reading mapping entries from Hive.
   * @param tableName Table name
   * @param partitionFilter Partition filter, or null if no filter used
   */
  public void setMappingInput(
      Class<? extends HiveToMapping> hiveToMappingClass, String tableName,
      String partitionFilter) {
    this.hiveToMappingClass = hiveToMappingClass;
    conf.set(HIVE_MAPPING_INPUT.getClassOpt().getKey(),
        hiveToMappingClass.getName());
    conf.set(HIVE_MAPPING_INPUT.getProfileIdOpt().getKey(),
        "mapping_input_profile");
    conf.set(HIVE_MAPPING_INPUT.getTableOpt().getKey(), tableName);
    if (partitionFilter != null) {
      conf.set(HIVE_MAPPING_INPUT.getPartitionOpt().getKey(), partitionFilter);
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
    HiveUtils.addHadoopClasspathToTmpJars(conf);
    HiveUtils.addHiveSiteXmlToTmpFiles(conf);

    // setup GiraphJob
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    GiraphConfiguration giraphConf = job.getConfiguration();
    giraphConf.setComputationClass(computationClass);

    giraphConf.setWorkerConfiguration(workers, workers, 100.0f);
    initGiraphJob(job);

    logOptions(giraphConf);

    return job.run(isVerbose) ? 0 : -1;
  }

  /**
   * Create ImmutableClassesGiraphConfiguration from provided Configuration
   * which is going to copy all the values set to it to this original
   * Configuration
   *
   * @param conf Configuration to create ImmutableClassesGiraphConfiguration
   *             from and update with any changes to the returned configuration
   * @return ImmutableClassesGiraphConfiguration
   */
  private ImmutableClassesGiraphConfiguration createGiraphConf(
      final Configuration conf) {
    return new ImmutableClassesGiraphConfiguration(conf) {
      @Override
      public void set(String name, String value) {
        super.set(name, value);
        conf.set(name, value);
      }
    };
  }

  /**
   * Create ImmutableClassesGiraphConfiguration from provided Configuration
   * which is going to copy all the values set to it to provided
   * InputFormatDescription
   *
   * @param conf Configuration to create ImmutableClassesGiraphConfiguration
   *             from
   * @param inputFormatDescription InputFormatDescription to update with any
   *                               changes to the returned configuration
   * @return ImmutableClassesGiraphConfiguration
   */
  private ImmutableClassesGiraphConfiguration createGiraphConf(
      Configuration conf,
      final InputFormatDescription inputFormatDescription) {
    return new ImmutableClassesGiraphConfiguration(conf) {
      @Override
      public void set(String name, String value) {
        super.set(name, value);
        inputFormatDescription.addParameter(name, value);
      }
    };
  }

  /**
   * Prepare vertex input settings in Configuration.
   *
   * For all Hive vertex inputs, add the user settings to the configuration.
   * Additionally, this checks the input specs for every input and caches
   * metadata information into the configuration to eliminate worker access to
   * the metastore and fail earlier in the case that metadata doesn't exist.
   * In the case of multiple vertex input descriptions, metadata is cached in
   * each vertex input format description and then saved into a single
   * Configuration via JSON.
   */
  @SuppressWarnings("unchecked")
  public void prepareHiveVertexInputs() {
    if (vertexInputDescriptions.size() == 1) {
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
          vertexInputDescriptions.get(0).getInputFormatClass());
      vertexInputDescriptions.get(0).putParametersToConfiguration(conf);
      // Create VertexInputFormat in order to initialize the Configuration with
      // data from metastore, and check it
      createGiraphConf(conf).createWrappedVertexInputFormat()
          .checkInputSpecs(conf);
    } else {
      // For each of the VertexInputFormats we'll prepare Configuration
      // parameters
      for (int i = 0; i < vertexInputDescriptions.size(); i++) {
        // Create a copy of the Configuration in order not to mess up the
        // original one
        Configuration confCopy = new Configuration(conf);
        final VertexInputFormatDescription vertexInputDescription =
            vertexInputDescriptions.get(i);
        GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(confCopy,
            vertexInputDescription.getInputFormatClass());
        vertexInputDescription.putParametersToConfiguration(confCopy);
        // Create VertexInputFormat in order to initialize its description with
        // data from metastore, and check it
        createGiraphConf(confCopy, vertexInputDescription)
            .createWrappedVertexInputFormat().checkInputSpecs(confCopy);
      }
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
          MultiVertexInputFormat.class);
      VertexInputFormatDescription.VERTEX_INPUT_FORMAT_DESCRIPTIONS.set(conf,
          InputFormatDescription.toJsonString(vertexInputDescriptions));
    }
  }

  /**
   * Prepare edge input settings in Configuration.
   *
   * For all Hive edge inputs, add the user settings to the configuration.
   * Additionally, this checks the input specs for every input and caches
   * metadata information into the configuration to eliminate worker access to
   * the metastore and fail earlier in the case that metadata doesn't exist.
   * In the case of multiple edge input descriptions, metadata is cached in each
   * vertex input format description and then saved into a single
   * Configuration via JSON.
   */
  @SuppressWarnings("unchecked")
  public void prepareHiveEdgeInputs() {
    if (edgeInputDescriptions.size() == 1) {
      GiraphConstants.EDGE_INPUT_FORMAT_CLASS.set(conf,
          edgeInputDescriptions.get(0).getInputFormatClass());
      edgeInputDescriptions.get(0).putParametersToConfiguration(conf);
      // Create EdgeInputFormat in order to initialize the Configuration with
      // data from metastore, and check it
      createGiraphConf(conf).createWrappedEdgeInputFormat()
          .checkInputSpecs(conf);
    } else {
      // For each of the EdgeInputFormats we'll prepare Configuration
      // parameters
      for (int i = 0; i < edgeInputDescriptions.size(); i++) {
        // Create a copy of the Configuration in order not to mess up the
        // original one
        Configuration confCopy = new Configuration(conf);
        final EdgeInputFormatDescription edgeInputDescription =
            edgeInputDescriptions.get(i);
        GiraphConstants.EDGE_INPUT_FORMAT_CLASS.set(confCopy,
            edgeInputDescription.getInputFormatClass());
        edgeInputDescription.putParametersToConfiguration(confCopy);
        // Create EdgeInputFormat in order to initialize its description with
        // data from metastore, and check it
        createGiraphConf(confCopy, edgeInputDescription)
            .createWrappedEdgeInputFormat().checkInputSpecs(confCopy);
      }
      GiraphConstants.EDGE_INPUT_FORMAT_CLASS.set(conf,
          MultiEdgeInputFormat.class);
      EdgeInputFormatDescription.EDGE_INPUT_FORMAT_DESCRIPTIONS.set(conf,
          InputFormatDescription.toJsonString(edgeInputDescriptions));
    }
  }

  /**
   * Prepare output settings in Configuration.
   *
   * This caches metadata information into the configuration to eliminate worker
   * access to the metastore.
   */
  public void prepareHiveOutput() {
    GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS.set(conf,
        HiveVertexOutputFormat.class);
    // Output format will be checked by Hadoop, here we only create it in
    // order to initialize the Configuration with data from metastore.
    // Can't check it here since we don't have JobContext yet
    createGiraphConf(conf).createWrappedVertexOutputFormat();
  }

  /**
   * Prepare input settings in Configuration
   *
   * This caches metadata information into the configuration to eliminate worker
   * access to the metastore.
   */
  public void prepareHiveMappingInput() {
    GiraphConstants.MAPPING_INPUT_FORMAT_CLASS.set(conf,
        HiveMappingInputFormat.class);

    Configuration confCopy = new Configuration(conf);
    createGiraphConf(confCopy)
        .createWrappedMappingInputFormat()
        .checkInputSpecs(confCopy);
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

    // pick up -hiveconf arguments (put -D arguments from command line to conf)
    HiveUtils.processHiveconfOptions(cmdln.getOptionValues("hiveconf"), conf);

    // Giraph classes
    String computationClassStr = cmdln.getOptionValue("computationClass");
    if (computationClassStr != null) {
      computationClass = findClass(computationClassStr, Computation.class);
    }
    if (computationClass == null) {
      throw new IllegalArgumentException(
          "Need the Giraph " + Computation.class.getSimpleName() +
              " class name (-computationClass) to use");
    }

    String mappingInput = cmdln.getOptionValue("mappingInput");
    if (mappingInput != null) {
      String[] parameters = split(mappingInput, ",", 3);
      if (parameters.length < 2) {
        throw new IllegalStateException("Illegal mappingInput description " +
            mappingInput + " - HiveToMapping class and table name needed");
      }
      setMappingInput(findClass(parameters[0], HiveToMapping.class),
          parameters[1], elementOrNull(parameters, 2));
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

    workers = Integer.parseInt(workersStr);

    isVerbose = cmdln.hasOption("verbose");

    // Processing more arguments should precede Hive preparation to
    // allow metastore changes (i.e. creating tables that don't exist)
    processMoreArguments(cmdln);

    if (mappingInput != null) { // mapping input is provided
      HIVE_MAPPING_INPUT.getDatabaseOpt().set(conf, dbName);
      prepareHiveMappingInput();
    }

    if (hasVertexInput()) {
      HIVE_VERTEX_INPUT.getDatabaseOpt().set(conf, dbName);
      prepareHiveVertexInputs();
    }

    if (hasEdgeInput()) {
      HIVE_EDGE_INPUT.getDatabaseOpt().set(conf, dbName);
      prepareHiveEdgeInputs();
    }

    if (!skipOutput) {
      HIVE_VERTEX_OUTPUT_DATABASE.set(conf, dbName);
      prepareHiveOutput();
    } else {
      LOG.warn("run: Warning - Output will be skipped!");
    }

    return cmdln;
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

    if (computationClass == null) {
      options.addOption(null, "computationClass", true,
          "Giraph Computation class to use");
    }

    options.addOption("db", "dbName", true, "Hive database name");

    // Mapping input settings
    options.addOption("mi", "mappingInput", true, "Giraph " +
        HiveToMapping.class.getSimpleName() + " class to use, table name and " +
        "partition filter (optional). Example:\n" +
        "\"MyHiveToMapping, myTableName, a=1,b=two");

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
    LOG.info(getClass().getSimpleName() + " with");

    LOG.info(LOG_PREFIX + "-computationClass=" +
        computationClass.getCanonicalName());

    for (VertexInputFormatDescription description : vertexInputDescriptions) {
      LOG.info(LOG_PREFIX + "Vertex input: " + description);
    }

    for (EdgeInputFormatDescription description : edgeInputDescriptions) {
      LOG.info(LOG_PREFIX + "Edge input: " + description);
    }

    if (GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS.contains(giraphConf)) {
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
