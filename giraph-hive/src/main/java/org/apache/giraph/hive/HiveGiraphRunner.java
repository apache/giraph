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
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.input.vertex.HiveVertexInputFormat;
import org.apache.giraph.hive.output.HiveVertexOutputFormat;
import org.apache.giraph.hive.output.HiveVertexWriter;
import org.apache.giraph.hive.output.VertexToHive;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.facebook.giraph.hive.input.HiveApiInputFormat;
import com.facebook.giraph.hive.input.HiveInputDescription;
import com.facebook.giraph.hive.output.HiveApiOutputFormat;
import com.facebook.giraph.hive.output.HiveOutputDescription;
import com.facebook.giraph.hive.schema.HiveTableSchemas;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_SPLITS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_EDGE_CLASS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_VERTEX_CLASS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_SPLITS;
import static org.apache.giraph.hive.common.HiveProfiles.EDGE_INPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.HiveProfiles.VERTEX_INPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.HiveProfiles.VERTEX_OUTPUT_PROFILE_ID;

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

  /** Vertex creator from hive records. */
  private Class<? extends HiveToVertex> hiveToVertexClass;
  /** hive vertex input information */
  private  final HiveInputDescription hiveVertexInputDescription;

  /** Edge creator from hive records. */
  private Class<? extends HiveToEdge> hiveToEdgeClass;
  /** hive edge input information */
  private final HiveInputDescription hiveEdgeInputDescription;

  /** Hive Vertex writer */
  private Class<? extends VertexToHive> vertexToHiveClass;
  /** hive output information */
  private final HiveOutputDescription hiveOutputDescription;
  /** Skip output? (Useful for testing without writing) */
  private boolean skipOutput = false;

  /** Configuration */
  private Configuration conf;

  /** Create a new runner */
  public HiveGiraphRunner() {
    conf = new HiveConf(getClass());
    hiveVertexInputDescription = new HiveInputDescription();
    hiveEdgeInputDescription = new HiveInputDescription();
    hiveOutputDescription = new HiveOutputDescription();
  }

  public Class<? extends Vertex> getVertexClass() {
    return vertexClass;
  }

  public void setVertexClass(Class<? extends Vertex> vertexClass) {
    this.vertexClass = vertexClass;
  }

  public HiveInputDescription getHiveVertexInputDescription() {
    return hiveVertexInputDescription;
  }

  public HiveOutputDescription getHiveOutputDescription() {
    return hiveOutputDescription;
  }

  public HiveInputDescription getHiveEdgeInputDescription() {
    return hiveEdgeInputDescription;
  }

  public Class<? extends HiveToVertex> getHiveToVertexClass() {
    return hiveToVertexClass;
  }

  /**
   * Set HiveToVertex used with HiveVertexInputFormat
   *
   * @param hiveToVertexClass HiveToVertex
   */
  public void setHiveToVertexClass(
      Class<? extends HiveToVertex> hiveToVertexClass) {
    this.hiveToVertexClass = hiveToVertexClass;
    HIVE_TO_VERTEX_CLASS.set(conf, hiveToVertexClass);
  }

  /**
   * Whether to use vertex input.
   *
   * @return true if vertex input enabled (HiveToVertex is set).
   */
  public boolean hasVertexValueInput() {
    return hiveToVertexClass != null;
  }

  public Class<? extends HiveToEdge> getHiveToEdgeClass() {
    return hiveToEdgeClass;
  }

  /**
   * Whether to use edge input.
   *
   * @return true if edge input enabled (HiveToEdge is set).
   */
  public boolean hasEdgeInput() {
    return hiveToEdgeClass != null;
  }

  /**
   * Set HiveToEdge used with HiveEdgeInputFormat
   *
   * @param hiveToEdgeClass HiveToEdge
   */
  public void setHiveToEdgeClass(Class<? extends HiveToEdge> hiveToEdgeClass) {
    this.hiveToEdgeClass = hiveToEdgeClass;
    HIVE_TO_EDGE_CLASS.set(conf, hiveToEdgeClass);
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
   * Set class used to write vertices to Hive.
   *
   * @param vertexToHiveClass class for writing vertices to Hive.
   */
  public void setVertexToHiveClass(
      Class<? extends VertexToHive> vertexToHiveClass) {
    this.vertexToHiveClass = vertexToHiveClass;
    conf.setClass(HiveVertexWriter.VERTEX_TO_HIVE_KEY, vertexToHiveClass,
        VertexToHive.class);
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

    setupHiveInputs(giraphConf);
    setupHiveOutput(giraphConf);

    giraphConf.setWorkerConfiguration(workers, workers, 100.0f);
    initGiraphJob(job);

    logOptions(giraphConf);

    return job.run(isVerbose) ? 0 : -1;
  }

  /**
   * Initialize hive input settings
   *
   * @param conf Configuration to write to
   * @throws TException thrift problem
   */
  private void setupHiveInputs(GiraphConfiguration conf) throws TException {
    if (hiveToVertexClass != null) {
      hiveVertexInputDescription.setNumSplits(HIVE_VERTEX_SPLITS.get(conf));
      HiveApiInputFormat.setProfileInputDesc(conf, hiveVertexInputDescription,
          VERTEX_INPUT_PROFILE_ID);
      conf.setVertexInputFormatClass(HiveVertexInputFormat.class);
      HiveTableSchemas.put(conf, VERTEX_INPUT_PROFILE_ID,
          hiveVertexInputDescription.hiveTableName());
    }

    if (hiveToEdgeClass != null) {
      hiveEdgeInputDescription.setNumSplits(HIVE_EDGE_SPLITS.get(conf));
      HiveApiInputFormat.setProfileInputDesc(conf, hiveEdgeInputDescription,
          EDGE_INPUT_PROFILE_ID);
      conf.setEdgeInputFormatClass(HiveEdgeInputFormat.class);
      HiveTableSchemas.put(conf, EDGE_INPUT_PROFILE_ID,
          hiveEdgeInputDescription.hiveTableName());
    }
  }

  /**
   * Initialize hive output settings
   *
   * @param conf Configuration to write to
   * @throws TException thrift problem
   */
  private void setupHiveOutput(GiraphConfiguration conf) throws TException {
    if (skipOutput) {
      LOG.warn("run: Warning - Output will be skipped!");
    } else if (vertexToHiveClass != null) {
      HiveApiOutputFormat.initProfile(conf, hiveOutputDescription,
          VERTEX_OUTPUT_PROFILE_ID);
      conf.setVertexOutputFormatClass(HiveVertexOutputFormat.class);
      HiveTableSchemas.put(conf, VERTEX_OUTPUT_PROFILE_ID,
          hiveOutputDescription.hiveTableName());
    } else {
      LOG.fatal("output requested but " + VertexToHive.class.getSimpleName() +
          " not set");
    }
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

    String hiveToVertexClassStr = cmdln.getOptionValue("hiveToVertexClass");
    if (hiveToVertexClassStr != null) {
      if (hiveToVertexClassStr.equals("disable")) {
        hiveToVertexClass = null;
      } else {
        setHiveToVertexClass(
            findClass(hiveToVertexClassStr, HiveToVertex.class));
      }
    }

    String hiveToEdgeClassStr = cmdln.getOptionValue("hiveToEdgeClass");
    if (hiveToEdgeClassStr != null) {
      if (hiveToEdgeClassStr.equals("disable")) {
        hiveToEdgeClass = null;
      } else {
        setHiveToEdgeClass(
            findClass(hiveToEdgeClassStr, HiveToEdge.class));
      }
    }

    String vertexToHiveClassStr = cmdln.getOptionValue("vertexToHiveClass");
    if (vertexToHiveClassStr != null) {
      setVertexToHiveClass(findClass(vertexToHiveClassStr, VertexToHive.class));
    }

    if (cmdln.hasOption("skipOutput")) {
      skipOutput = true;
    }

    if (hiveToVertexClass == null && hiveToEdgeClass == null) {
      throw new IllegalArgumentException(
          "Need at least one of Giraph " +
          HiveToVertex.class.getSimpleName() +
          " class name (-hiveToVertexClass) and " +
          HiveToEdge.class.getSimpleName() +
          " class name (-hiveToEdgeClass)");
    }
    if (vertexToHiveClass == null && !skipOutput) {
      throw new IllegalArgumentException(
          "Need the Giraph " + VertexToHive.class.getSimpleName() +
          " class name (-vertexToHiveClass) to use");
    }
    String workersStr = cmdln.getOptionValue("workers");
    if (workersStr == null) {
      throw new IllegalArgumentException(
          "Need to choose the number of workers (-w)");
    }

    String vertexInputTableStr = cmdln.getOptionValue("vertexInputTable");
    if (vertexInputTableStr == null && hiveToVertexClass != null) {
      throw new IllegalArgumentException(
          "Need to set the vertex input table name (-vi)");
    }

    String edgeInputTableStr = cmdln.getOptionValue("edgeInputTable");
    if (edgeInputTableStr == null && hiveToEdgeClass != null) {
      throw new IllegalArgumentException(
          "Need to set the edge input table name (-ei)");
    }

    String outputTableStr = cmdln.getOptionValue("outputTable");
    if (outputTableStr == null) {
      throw new IllegalArgumentException(
          "Need to set the output table name (-o)");
    }

    String dbName = cmdln.getOptionValue("dbName", "default");
    hiveVertexInputDescription.setDbName(dbName);
    hiveEdgeInputDescription.setDbName(dbName);
    hiveOutputDescription.setDbName(dbName);

    hiveEdgeInputDescription.setPartitionFilter(
        cmdln.getOptionValue("edgeInputFilter"));
    hiveEdgeInputDescription.setTableName(edgeInputTableStr);

    hiveVertexInputDescription.setPartitionFilter(
        cmdln.getOptionValue("vertexInputFilter"));
    hiveVertexInputDescription.setTableName(vertexInputTableStr);

    hiveOutputDescription.setTableName(cmdln.getOptionValue("outputTable"));
    hiveOutputDescription.setPartitionValues(
        parsePartitionValues(cmdln.getOptionValue("outputPartition"))
    );

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
   * @param outputTablePartitionString table partition string
   * @return Map
   */
  public static Map<String, String> parsePartitionValues(
      String outputTablePartitionString) {
    if (outputTablePartitionString == null) {
      return null;
    }
    Splitter commaSplitter = Splitter.on(',').omitEmptyStrings().trimResults();
    Splitter equalSplitter = Splitter.on('=').omitEmptyStrings().trimResults();
    Map<String, String> partitionValues = Maps.newHashMap();
    for (String keyValStr : commaSplitter.split(outputTablePartitionString)) {
      List<String> keyVal = Lists.newArrayList(equalSplitter.split(keyValStr));
      if (keyVal.size() != 2) {
        throw new IllegalArgumentException(
            "Unrecognized partition value format: " +
            outputTablePartitionString);
      }
      partitionValues.put(keyVal.get(0), keyVal.get(1));
    }
    return partitionValues;
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
    options.addOption(null, "hiveToVertexClass", true,
        "Giraph " + HiveToVertex.class.getSimpleName() +
            " class to use (default - " +
            (hiveToVertexClass == null ? "not used" :
                hiveToVertexClass.getSimpleName()) + "), " +
            "\"disable\" will unset this option");
    options.addOption("vi", "vertexInputTable", true,
        "Vertex input table name");
    options.addOption("VI", "vertexInputFilter", true,
        "Vertex input table filter expression (e.g., \"a<2 AND b='two'\"");

    // Edge input settings
    options.addOption(null, "hiveToEdgeClass", true,
        "Giraph " + HiveToEdge.class.getSimpleName() +
            " class to use (default - " +
            (hiveToEdgeClass == null ? "not used" :
                hiveToEdgeClass.getSimpleName()) + "), " +
            "\"disable\" will unset this option");
    options.addOption("ei", "edgeInputTable", true,
        "Edge input table name");
    options.addOption("EI", "edgeInputFilter", true,
        "Edge input table filter expression (e.g., \"a<2 AND b='two'\"");

    // Vertex output settings
    if (vertexToHiveClass == null) {
      options.addOption(null, "vertexToHiveClass", true,
          "Giraph " + VertexToHive.class.getSimpleName() + " class to use");
    }

    options.addOption("o", "outputTable", true, "Output table name");
    options.addOption("O", "outputPartition", true,
        "Output table partition values (e.g., \"a=1,b=two\")");
    options.addOption("s", "skipOutput", false, "Skip output?");
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
    this.conf = conf;
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
    GiraphClasses classes = new GiraphClasses(giraphConf);

    LOG.info(getClass().getSimpleName() + " with");

    LOG.info(LOG_PREFIX + "-vertexClass=" + vertexClass.getCanonicalName());

    if (hiveToVertexClass != null) {
      LOG.info(LOG_PREFIX + "-hiveToVertexClass=" +
          hiveToVertexClass.getCanonicalName());
    }
    if (classes.getVertexInputFormatClass() != null) {
      LOG.info(LOG_PREFIX + "-vertexInputFormatClass=" +
          classes.getVertexInputFormatClass().getCanonicalName());
      logInputDesc(hiveVertexInputDescription, "vertex");
    }

    if (hiveToEdgeClass != null) {
      LOG.info(LOG_PREFIX + "-hiveToEdgeClass=" +
          hiveToEdgeClass.getCanonicalName());
    }
    if (classes.getEdgeInputFormatClass() != null) {
      LOG.info(LOG_PREFIX + "-edgeInputFormatClass=" +
        classes.getEdgeInputFormatClass().getCanonicalName());
      logInputDesc(hiveEdgeInputDescription, "edge");
    }

    LOG.info(LOG_PREFIX + "-outputTable=" +
        hiveOutputDescription.getTableName());
    if (hiveOutputDescription.hasPartitionValues()) {
      LOG.info(LOG_PREFIX + "-outputPartition=\"" +
          hiveOutputDescription.getPartitionValues() + "\"");
    }
    if (classes.getVertexOutputFormatClass() != null) {
      LOG.info(LOG_PREFIX + "-outputFormatClass=" +
          classes.getVertexOutputFormatClass().getCanonicalName());
    }

    LOG.info(LOG_PREFIX + "-workers=" + workers);
  }

  /**
   * Helper to log input description with a name
   *
   * @param inputDesc input description to log
   * @param name String prefix name
   */
  private void logInputDesc(HiveInputDescription inputDesc, String name) {
    if (inputDesc.hasTableName()) {
      LOG.info(
          LOG_PREFIX + "-" + name + "InputTable=" + inputDesc.getTableName());
    }
    if (inputDesc.hasPartitionFilter()) {
      LOG.info(LOG_PREFIX + "-" + name + "InputFilter=\"" +
          inputDesc.getPartitionFilter() + "\"");
    }
  }
}
