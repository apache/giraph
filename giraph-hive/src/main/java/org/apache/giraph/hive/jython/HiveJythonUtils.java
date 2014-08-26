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
package org.apache.giraph.hive.jython;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.GiraphTypes;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.graph.Language;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.hive.common.LanguageAndType;
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat;
import org.apache.giraph.hive.input.vertex.HiveVertexInputFormat;
import org.apache.giraph.hive.output.HiveVertexOutputFormat;
import org.apache.giraph.hive.primitives.PrimitiveValueReader;
import org.apache.giraph.hive.primitives.PrimitiveValueWriter;
import org.apache.giraph.hive.values.HiveValueReader;
import org.apache.giraph.hive.values.HiveValueWriter;
import org.apache.giraph.io.formats.multi.MultiEdgeInputFormat;
import org.apache.giraph.io.formats.multi.MultiVertexInputFormat;
import org.apache.giraph.jython.factories.JythonEdgeValueFactory;
import org.apache.giraph.jython.factories.JythonFactoryBase;
import org.apache.giraph.jython.factories.JythonIncomingMessageValueFactory;
import org.apache.giraph.jython.JythonJob;
import org.apache.giraph.jython.factories.JythonOutgoingMessageValueFactory;
import org.apache.giraph.jython.JythonUtils;
import org.apache.giraph.jython.factories.JythonVertexIdFactory;
import org.apache.giraph.jython.factories.JythonVertexValueFactory;
import org.apache.giraph.jython.wrappers.JythonWritableWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.python.core.Py;
import org.python.core.PyClass;
import org.python.core.PyObject;
import org.python.core.PyType;
import org.python.util.PythonInterpreter;

import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.giraph.conf.GiraphConstants.EDGE_INPUT_FORMAT_CLASS;
import static org.apache.giraph.conf.GiraphConstants.GRAPH_TYPE_LANGUAGES;
import static org.apache.giraph.conf.GiraphConstants.MAX_WORKERS;
import static org.apache.giraph.conf.GiraphConstants.MIN_WORKERS;
import static org.apache.giraph.conf.GiraphConstants.MESSAGE_COMBINER_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_INPUT_FORMAT_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_DATABASE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PARTITION;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PROFILE_ID;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_OUTPUT_TABLE;
import static org.apache.giraph.hive.common.GiraphHiveConstants.VERTEX_TO_HIVE_CLASS;
import static org.apache.giraph.hive.common.GiraphHiveConstants.VERTEX_VALUE_READER_JYTHON_NAME;
import static org.apache.giraph.hive.common.GiraphHiveConstants.VERTEX_VALUE_WRITER_JYTHON_NAME;
import static org.apache.giraph.hive.jython.JythonHiveToEdge.EDGE_SOURCE_ID_COLUMN;
import static org.apache.giraph.hive.jython.JythonHiveToEdge.EDGE_TARGET_ID_COLUMN;
import static org.apache.giraph.hive.jython.JythonHiveToEdge.EDGE_VALUE_COLUMN;
import static org.apache.giraph.hive.jython.JythonVertexToHive.VERTEX_VALUE_COLUMN;

/**
 * Plugin to {@link HiveJythonRunner} to use Hive.
 */
public class HiveJythonUtils {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(HiveJythonUtils.class);

  /** Don't construct */
  private HiveJythonUtils() { }

  /**
   * Process command line arguments
   *
   * @param args cmdline args
   * @param conf {@link Configuration}
   * @return remaining cmdline args to process
   */
  public static String[] processArgs(String[] args, Configuration conf) {
    HiveUtils.addHadoopClasspathToTmpJars(conf);
    HiveUtils.addHiveSiteXmlToTmpFiles(conf);
    HiveUtils.addHiveSiteCustomXmlToTmpFiles(conf);
    return moveHiveconfOptionsToConf(args, conf);
  }

  /**
   * Remove -hiveconf options from cmdline
   *
   * @param args cmdline args
   * @param conf Configuration
   * @return cmdline args without -hiveconf options
   */
  private static String[] moveHiveconfOptionsToConf(String[] args,
      Configuration conf) {
    int start = 0;
    while (start < args.length) {
      if (args[start].endsWith("hiveconf")) {
        HiveUtils.processHiveconfOption(conf, args[start + 1]);
        start += 2;
      } else {
        break;
      }
    }
    return Arrays.copyOfRange(args, start, args.length);
  }

  /**
   * Parse set of Jython scripts from local files
   *
   * @param interpreter PythonInterpreter to use
   * @param paths Jython files to parse
   * @return JythonJob
   * @throws java.io.IOException
   */
  public static JythonJob parseJythonFiles(PythonInterpreter interpreter,
      String ... paths) throws IOException {
    return parseJythonFiles(interpreter, Arrays.asList(paths));
  }

  /**
   * Parse set of Jython scripts from local files
   *
   * @param interpreter PythonInterpreter to use
   * @param paths Jython files to parse
   * @return JythonJob
   * @throws IOException
   */
  public static JythonJob parseJythonFiles(PythonInterpreter interpreter,
      List<String> paths) throws IOException {
    InputStream[] streams = new InputStream[paths.size()];
    for (int i = 0; i < paths.size(); ++i) {
      LOG.info("Reading jython file " + paths.get(i));
      streams[i] = new FileInputStream(paths.get(i));
    }

    JythonJob jythonJob;
    try {
      jythonJob = parseJythonStreams(interpreter, streams);
    } finally {
      for (InputStream stream : streams) {
        Closeables.close(stream, true);
      }
    }
    return jythonJob;
  }

  /**
   * Parse scripts from Jython InputStreams
   *
   * @param interpreter PythonInterpreter
   * @param streams InputStreams to parse
   * @return JythonJob
   */
  public static JythonJob parseJythonStreams(PythonInterpreter interpreter,
      InputStream ... streams) {
    for (InputStream stream : streams) {
      readJythonStream(interpreter, stream);
    }

    PyObject pyPrepare = interpreter.get("prepare");

    JythonJob jythonJob = new JythonJob();
    pyPrepare._jcall(new Object[]{jythonJob});

    return jythonJob;
  }

  /**
   * Execute a Jython script
   *
   * @param interpreter Jython interpreter to use
   * @param jythonStream {@link java.io.InputStream} with Jython code
   * @throws java.io.IOException
   */
  private static void readJythonStream(PythonInterpreter interpreter,
      InputStream jythonStream) {
    try {
      interpreter.execfile(jythonStream);
    } finally {
      try {
        jythonStream.close();
      } catch (IOException e) {
        LOG.error("Failed to close jython stream " + jythonStream);
      }
    }
  }

  /**
   * Set arbitrary option of unknown type in Configuration
   *
   * @param conf Configuration
   * @param key String key
   * @param value Object to set
   */
  private static void setOption(Configuration conf, String key,
      Object value) {
    if (value instanceof Boolean) {
      conf.getBoolean(key, (Boolean) value);
    } else if (value instanceof Byte || value instanceof Short ||
        value instanceof Integer) {
      conf.setInt(key, ((Number) value).intValue());
    } else if (value instanceof Long) {
      conf.setLong(key, (Long) value);
    } else if (value instanceof Float || value instanceof Double) {
      conf.setFloat(key, ((Number) value).floatValue());
    } else if (value instanceof String) {
      conf.set(key, value.toString());
    } else if (value instanceof Class) {
      conf.set(key, ((Class) value).getName());
    } else {
      throw new IllegalArgumentException(
          "Don't know how to handle option key: " + key +
          ", value: " + value + ", value type: " + value.getClass());
    }
  }

  /**
   * Write JythonJob to Configuration
   *
   * @param jythonJob JythonJob
   * @param conf Configuration
   * @param interpreter PythonInterpreter
   * @return name of Job
   */
  public static String writeJythonJobToConf(JythonJob jythonJob,
      Configuration conf, PythonInterpreter interpreter) {
    checkJob(jythonJob);

    JythonUtils.init(conf, jythonJob.getComputation_name());

    if (jythonJob.getMessageCombiner() != null) {
      MESSAGE_COMBINER_CLASS.set(conf, jythonJob.getMessageCombiner());
    }

    conf.setInt(MIN_WORKERS, jythonJob.getWorkers());
    conf.setInt(MAX_WORKERS, jythonJob.getWorkers());

    String javaOptions = Joiner.on(' ').join(jythonJob.getJava_options());
    conf.set("mapred.child.java.opts", javaOptions);

    Map<String, Object> options = jythonJob.getGiraph_options();
    for (Map.Entry<String, Object> entry : options.entrySet()) {
      setOption(conf, entry.getKey(), entry.getValue());
    }

    setPool(conf, jythonJob);

    initHiveReadersWriters(conf, jythonJob, interpreter);
    initGraphTypes(conf, jythonJob, interpreter);
    initOutput(conf, jythonJob);
    initVertexInputs(conf, jythonJob);
    initEdgeInputs(conf, jythonJob);

    String name = jythonJob.getName();
    if (name == null) {
      name = jythonJob.getComputation_name();
    }
    return name;
  }

  /**
   * Set the hadoop mapreduce pool
   *
   * @param conf Configuration
   * @param job the job info
   */
  private static void setPool(Configuration conf, JythonJob job) {
    if (job.getPool() == null) {
      if (job.getWorkers() < 50) {
        job.setPool("graph.test");
      } else {
        job.setPool("graph.production");
      }
    }
    conf.set("mapred.fairscheduler.pool", job.getPool());
  }

  /**
   * Check that the job is valid
   *
   * @param jythonJob JythonJob
   */
  private static void checkJob(JythonJob jythonJob) {
    checkNotNull(jythonJob.getComputation_name(),
        "computation_name cannot be null");
    checkTypeNotNull(jythonJob.getVertex_id(), GraphType.VERTEX_ID);
    checkTypeNotNull(jythonJob.getVertex_value(), GraphType.VERTEX_VALUE);
    checkTypeNotNull(jythonJob.getEdge_value(), GraphType.EDGE_VALUE);
    checkMessageTypes(jythonJob);
  }

  /**
   * Check if job has edge inputs
   *
   * @param jythonJob JythonJob
   * @return true if job has edge inputs, false otherwise
   */
  private static boolean hasEdgeInputs(JythonJob jythonJob) {
    return !jythonJob.getEdge_inputs().isEmpty();
  }

  /**
   * Check if job has vertex inputs
   *
   * @param jythonJob JythonJob
   * @return true if job has vertex inputs, false otherwise
   */
  private static boolean hasVertexInputs(JythonJob jythonJob) {
    return !jythonJob.getVertex_inputs().isEmpty();
  }

  /**
   * Check that type is present
   *
   * @param typeHolder TypeHolder
   * @param graphType GraphType
   */
  private static void checkTypeNotNull(JythonJob.TypeHolder typeHolder,
      GraphType graphType) {
    checkNotNull(typeHolder.getType(), graphType + ".type not present");
  }

  /**
   * Initialize the job types
   *
   * @param conf Configuration
   * @param jythonJob the job info
   * @param interpreter PythonInterpreter to use
   */
  private static void initGraphTypes(Configuration conf,
      JythonJob jythonJob, PythonInterpreter interpreter) {
    GiraphTypes types = new GiraphTypes();
    types.setVertexIdClass(initValueType(conf, GraphType.VERTEX_ID,
        jythonJob.getVertex_id().getType(), new JythonVertexIdFactory(),
        interpreter));
    types.setVertexValueClass(initValueType(conf, GraphType.VERTEX_VALUE,
        jythonJob.getVertex_value().getType(), new JythonVertexValueFactory(),
        interpreter));
    types.setEdgeValueClass(initValueType(conf, GraphType.EDGE_VALUE,
        jythonJob.getEdge_value().getType(), new JythonEdgeValueFactory(),
        interpreter));
    types.setIncomingMessageValueClass(
        initValueType(conf, GraphType.INCOMING_MESSAGE_VALUE,
            jythonJob.getIncoming_message_value().getType(),
            new JythonIncomingMessageValueFactory(), interpreter));
    types.setOutgoingMessageValueClass(
        initValueType(conf, GraphType.OUTGOING_MESSAGE_VALUE,
            jythonJob.getOutgoing_message_value().getType(),
            new JythonOutgoingMessageValueFactory(), interpreter));
    types.writeTo(conf);
  }

  /**
   * Initialize a graph type (IVEMM)
   *
   * @param conf Configuration
   * @param graphType GraphType
   * @param jythonOrJavaClass jython or java class given by user
   * @param jythonFactory Jactory for making Jython types
   * @param interpreter PythonInterpreter
   * @return Class for Configuration
   */
  private static Class initValueType(Configuration conf, GraphType graphType,
      Object jythonOrJavaClass, JythonFactoryBase jythonFactory,
      PythonInterpreter interpreter) {
    Class<? extends Writable> writableClass = graphType.interfaceClass();
    LanguageAndType langType = processUserType(jythonOrJavaClass, interpreter);

    switch (langType.getLanguage()) {
    case JAVA:
      GRAPH_TYPE_LANGUAGES.set(conf, graphType, Language.JAVA);
      checkImplements(langType, writableClass, interpreter);
      return langType.getJavaClass();
    case JYTHON:
      GRAPH_TYPE_LANGUAGES.set(conf, graphType, Language.JYTHON);
      String jythonClassName = langType.getJythonClassName();
      PyObject jythonClass = interpreter.get(jythonClassName);
      if (jythonClass == null) {
        throw new IllegalArgumentException("Could not find Jython class " +
            jythonClassName + " for parameter " + graphType);
      }
      PyObject valuePyObj = jythonClass.__call__();

      // Check if the Jython type implements Writable. If so, just use it
      // directly. Otherwise, wrap it in a class that does using pickle.
      Object pyWritable = valuePyObj.__tojava__(writableClass);
      if (pyWritable.equals(Py.NoConversion)) {
        GiraphConstants.GRAPH_TYPES_NEEDS_WRAPPERS.set(conf, graphType, true);
        jythonFactory.useThisFactory(conf, jythonClassName);
        return JythonWritableWrapper.class;
      } else {
        GiraphConstants.GRAPH_TYPES_NEEDS_WRAPPERS.set(conf, graphType, false);
        jythonFactory.useThisFactory(conf, jythonClassName);
        return writableClass;
      }
    default:
      throw new IllegalArgumentException("Don't know how to handle " +
          LanguageAndType.class.getSimpleName() + " with language " +
          langType.getLanguage());
    }
  }

  /**
   * Check that the incoming / outgoing message value types are present.
   *
   * @param jythonJob JythonJob
   */
  private static void checkMessageTypes(JythonJob jythonJob) {
    checkMessageType(jythonJob.getIncoming_message_value(),
        GraphType.INCOMING_MESSAGE_VALUE, jythonJob);
    checkMessageType(jythonJob.getOutgoing_message_value(),
        GraphType.OUTGOING_MESSAGE_VALUE, jythonJob);
  }

  /**
   * Check that given message value type is present.
   *
   * @param msgTypeHolder Incoming or outgoing message type holder
   * @param graphType The graph type
   * @param jythonJob JythonJob
   */
  private static void checkMessageType(JythonJob.TypeHolder msgTypeHolder,
      GraphType graphType, JythonJob jythonJob) {
    if (msgTypeHolder.getType() == null) {
      Object msgValueType = jythonJob.getMessage_value().getType();
      checkNotNull(msgValueType, graphType + ".type and " +
          "message_value.type cannot both be empty");
      msgTypeHolder.setType(msgValueType);
    }
  }

  /**
   * Check that the vertex ID, vertex value, and edge value Hive info is valid.
   *
   * @param conf Configuration
   * @param jythonJob JythonJob
   * @param interpreter PythonInterpreter
   */
  private static void initHiveReadersWriters(Configuration conf,
      JythonJob jythonJob, PythonInterpreter interpreter) {
    if (!userTypeIsJavaPrimitiveWritable(jythonJob.getVertex_id())) {
      checkTypeWithHive(jythonJob.getVertex_id(), GraphType.VERTEX_ID);

      LanguageAndType idReader = processUserType(
          jythonJob.getVertex_id().getHive_reader(), interpreter);
      checkImplements(idReader, JythonHiveReader.class, interpreter);
      checkArgument(idReader.getLanguage() == Language.JYTHON);
      GiraphHiveConstants.VERTEX_ID_READER_JYTHON_NAME.set(conf,
          idReader.getJythonClassName());

      LanguageAndType idWriter = processUserType(
          jythonJob.getVertex_id().getHive_writer(), interpreter);
      checkImplements(idWriter, JythonHiveWriter.class, interpreter);
      checkArgument(idWriter.getLanguage() == Language.JYTHON);
      GiraphHiveConstants.VERTEX_ID_WRITER_JYTHON_NAME.set(conf,
          idWriter.getJythonClassName());
    }

    if (hasVertexInputs(jythonJob) &&
        !userTypeIsJavaPrimitiveWritable(jythonJob.getVertex_value())) {
      checkTypeWithHive(jythonJob.getVertex_value(), GraphType.VERTEX_VALUE);

      LanguageAndType valueReader = processUserType(
          jythonJob.getVertex_value().getHive_reader(), interpreter);
      checkImplements(valueReader, JythonHiveReader.class, interpreter);
      checkArgument(valueReader.getLanguage() == Language.JYTHON);
      VERTEX_VALUE_READER_JYTHON_NAME.set(conf,
          valueReader.getJythonClassName());

      LanguageAndType valueWriter = processUserType(
          jythonJob.getVertex_value().getHive_writer(), interpreter);
      checkImplements(valueWriter, JythonHiveWriter.class, interpreter);
      checkArgument(valueWriter.getLanguage() == Language.JYTHON);
      VERTEX_VALUE_WRITER_JYTHON_NAME.set(conf,
          valueWriter.getJythonClassName());
    }

    if (hasEdgeInputs(jythonJob) &&
        !userTypeIsJavaPrimitiveWritable(jythonJob.getEdge_value())) {
      checkNotNull(jythonJob.getEdge_value().getHive_reader(),
          "edge_value.hive_reader cannot be null");

      LanguageAndType edgeReader = processUserType(
          jythonJob.getEdge_value().getHive_reader(), interpreter);
      checkImplements(edgeReader, JythonHiveReader.class, interpreter);
      checkArgument(edgeReader.getLanguage() == Language.JYTHON);
      GiraphHiveConstants.EDGE_VALUE_READER_JYTHON_NAME.set(conf,
          edgeReader.getJythonClassName());
    }
  }

  /**
   * Verify Jython class is present and implements the Java type
   *
   * @param interpreter PythonInterpreter
   * @param valueFromUser Jython class or name of class
   * @return name of Jython class
   */
  private static LanguageAndType processUserType(Object valueFromUser,
      PythonInterpreter interpreter) {
    // user gave a Class object, should be either Java or Jython class name
    if (valueFromUser instanceof Class) {
      Class valueClass = (Class) valueFromUser;
      String jythonClassName = extractJythonClass(valueClass);
      if (jythonClassName != null) {
        // Jython class
        return processJythonType(jythonClassName, interpreter);
      } else {
        // Java class
        return LanguageAndType.java(valueClass);
      }

      // user gave a string, should be either Java or Jython class name
    } else if (valueFromUser instanceof String) {
      String valueStr = (String) valueFromUser;
      Class valueClass;
      try {
        // Try to find Java class with name
        valueClass = Class.forName(valueStr);
        return LanguageAndType.java(valueClass);
      } catch (ClassNotFoundException e) {
        // Java class not found, try to find a Jython one
        return processJythonType(valueStr, interpreter);
      }

      // user gave a PyClass, process as a Jython class
    } else if (valueFromUser instanceof PyClass) {
      PyClass userPyClass = (PyClass) valueFromUser;
      return processJythonType(userPyClass.__name__, interpreter);

      // user gave a PyType, process as Jython class
    } else if (valueFromUser instanceof PyType) {
      PyType userPyType = (PyType) valueFromUser;
      return processJythonType(userPyType.getName(), interpreter);

      // Otherwise, don't know how to handle this, so error
    } else {
      throw new IllegalArgumentException("Don't know how to handle " +
          valueFromUser + " of class " + valueFromUser.getClass() +
          ", needs to be Class or String");
    }
  }

  /**
   * Check that a type implements a Java interface
   *
   * @param langType type with langauge
   * @param interfaceClass java interface class
   * @param interpreter PythonInterpreter
   */
  private static void checkImplements(LanguageAndType langType,
      Class interfaceClass, PythonInterpreter interpreter) {
    switch (langType.getLanguage()) {
    case JAVA:
      checkArgument(interfaceClass.isAssignableFrom(langType.getJavaClass()),
          langType.getJavaClass().getSimpleName() + " needs to implement " +
          interfaceClass.getSimpleName());
      break;
    case JYTHON:
      PyObject pyClass = interpreter.get(langType.getJythonClassName());
      PyObject pyObj = pyClass.__call__();
      Object converted = pyObj.__tojava__(interfaceClass);
      checkArgument(!Py.NoConversion.equals(converted),
         "Jython class " + langType.getJythonClassName() +
         " does not implement " + interfaceClass.getSimpleName() +
         " interface");
      break;
    default:
      throw new IllegalArgumentException("Don't know how to handle " +
          "language " + langType.getLanguage());
    }
  }

  /**
   * Verify Jython class is present and implements specified type
   *
   * @param jythonName Jython class name
   * @param interpreter PythonInterpreter
   * @return language and type specification
   */
  private static LanguageAndType processJythonType(String jythonName,
      PythonInterpreter interpreter) {
    PyObject pyClass = interpreter.get(jythonName);
    checkNotNull(pyClass, "Jython class " + jythonName + " not found");
    return LanguageAndType.jython(jythonName);
  }

  /**
   * Check that the given value type is valid
   *
   * @param typeWithHive value type
   * @param graphType GraphType
   */
  private static void checkTypeWithHive(JythonJob.TypeWithHive typeWithHive,
      GraphType graphType) {
    if (typeWithHive.getHive_reader() == null) {
      checkNotNull(typeWithHive.getHive_io(), graphType + ".hive_reader and " +
          graphType + ".hive_io cannot both be empty");
      typeWithHive.setHive_reader(typeWithHive.getHive_io());
    }
    if (typeWithHive.getHive_writer() == null) {
      checkNotNull(typeWithHive.getHive_io(), graphType + ".hive_writer and " +
          graphType + ".hive_io cannot both be empty");
      typeWithHive.setHive_writer(typeWithHive.getHive_io());
    }
  }

  /**
   * Create a graph value (IVEMM) reader
   *
   * @param <T> graph value type
   * @param schema {@link com.facebook.hiveio.schema.HiveTableSchema}
   * @param columnOption option for column name
   * @param conf {@link ImmutableClassesGiraphConfiguration}
   * @param graphType GraphType creating a reader for
   * @param jythonClassNameOption option for jython class option
   * @return {@link org.apache.giraph.hive.values.HiveValueReader}
   */
  public static <T extends Writable> HiveValueReader<T> newValueReader(
      HiveTableSchema schema, StrConfOption columnOption,
      ImmutableClassesGiraphConfiguration conf, GraphType graphType,
      StrConfOption jythonClassNameOption) {
    HiveValueReader<T> reader;
    if (HiveJythonUtils.isPrimitiveWritable(graphType.get(conf))) {
      reader = PrimitiveValueReader.create(conf, graphType, columnOption,
          schema);
    } else if (jythonClassNameOption.contains(conf)) {
      reader = JythonColumnReader.create(conf, jythonClassNameOption,
          columnOption, schema);
    } else {
      throw new IllegalArgumentException("Don't know how to read " + graphType +
          " of class " + graphType.get(conf) + " which is not primitive and" +
          " no " + JythonHiveReader.class.getSimpleName() + " is set");
    }
    return reader;
  }

  /**
   * Create a graph value (IVEMM) writer
   *
   * @param <T> writable type
   * @param schema {@link HiveTableSchema}
   * @param columnOption option for column
   * @param conf {@link ImmutableClassesGiraphConfiguration}
   * @param graphType {@link GraphType}
   * @param jythonClassNameOption option for name of jython class
   * @return {@link HiveValueWriter}
   */
  public static <T extends Writable> HiveValueWriter<T>
  newValueWriter(HiveTableSchema schema, StrConfOption columnOption,
      ImmutableClassesGiraphConfiguration conf, GraphType graphType,
      StrConfOption jythonClassNameOption) {
    HiveValueWriter<T> writer;
    if (HiveJythonUtils.isPrimitiveWritable(graphType.get(conf))) {
      writer = PrimitiveValueWriter.create(conf, columnOption,
          schema, graphType);
    } else if (jythonClassNameOption.contains(conf)) {
      writer = JythonColumnWriter.create(conf, jythonClassNameOption,
          columnOption, schema);
    } else {
      throw new IllegalArgumentException("Don't know how to write " +
          graphType +  " of class " + graphType.get(conf) +
          " which is not primitive and no " +
          JythonHiveWriter.class.getSimpleName() + " is set");
    }
    return writer;
  }

  /**
   * Extract Jython class name from a user set proxy Jython class.
   *
   * For example:
   *    job.vertex_value_type = FakeLPVertexValue
   * Yields:
   *    org.python.proxies.__main__$FakeLPVertexValue$0
   * This method extracts:
   *    FakeLPVertexValue
   *
   * @param klass Jython proxy class
   * @return Jython class name
   */
  private static String extractJythonClass(Class klass) {
    if (!isJythonClass(klass)) {
      return null;
    }
    Iterable<String> parts = Splitter.on('$').split(klass.getSimpleName());
    if (Iterables.size(parts) != 3) {
      return null;
    }
    Iterator<String> partsIter = parts.iterator();
    partsIter.next();
    return partsIter.next();
  }

  /**
   * Check if passed in class is a Jython class
   *
   * @param klass to check
   * @return true if Jython class, false otherwise
   */
  private static boolean isJythonClass(Class klass) {
    return klass.getCanonicalName().startsWith("org.python.proxies");
  }

  /**
   * Initialize edge input
   *
   * @param conf Configuration
   * @param jythonJob data to initialize
   */
  private static void initEdgeInputs(Configuration conf, JythonJob jythonJob) {
    List<JythonJob.EdgeInput> edgeInputs = jythonJob.getEdge_inputs();
    if (!edgeInputs.isEmpty()) {
      if (edgeInputs.size() == 1) {
        EDGE_INPUT_FORMAT_CLASS.set(conf, HiveEdgeInputFormat.class);
        JythonJob.EdgeInput edgeInput = edgeInputs.get(0);
        checkEdgeInput(edgeInput);
        LOG.info("Setting edge input using: " + edgeInput);

        HIVE_EDGE_INPUT.getDatabaseOpt().set(conf,
            jythonJob.getHive_database());
        HIVE_EDGE_INPUT.getTableOpt().set(conf, edgeInput.getTable());
        if (edgeInput.getPartition_filter() != null) {
          HIVE_EDGE_INPUT.getPartitionOpt().set(conf,
              edgeInput.getPartition_filter());
        }
        HIVE_EDGE_INPUT.getClassOpt().set(conf, JythonHiveToEdge.class);

        EDGE_SOURCE_ID_COLUMN.set(conf, edgeInput.getSource_id_column());
        EDGE_TARGET_ID_COLUMN.set(conf, edgeInput.getTarget_id_column());
        if (edgeInput.getValue_column() != null) {
          EDGE_VALUE_COLUMN.set(conf, edgeInput.getValue_column());
        }
      } else {
        EDGE_INPUT_FORMAT_CLASS.set(conf, MultiEdgeInputFormat.class);
        throw new IllegalArgumentException(
            "Multiple edge inputs not supported yet: " + edgeInputs);
      }
    }
  }

  /**
   * Check that the edge input is valid
   *
   * @param edgeInput data to check
   */
  private static void checkEdgeInput(JythonJob.EdgeInput edgeInput) {
    checkNotNull(edgeInput.getTable(), "EdgeInput table name needs to be set");
    checkNotNull(edgeInput.getSource_id_column(),
        "EdgeInput source ID column needs to be set");
    checkNotNull(edgeInput.getTarget_id_column(),
        "EdgeInput target ID column needs to be set");
  }

  /**
   * Initialize vertex output info
   *
   * @param conf Configuration
   * @param jythonJob the job info
   */
  private static void initVertexInputs(Configuration conf,
      JythonJob jythonJob) {
    List<JythonJob.VertexInput> vertexInputs = jythonJob.getVertex_inputs();
    if (!vertexInputs.isEmpty()) {
      if (vertexInputs.size() == 1) {
        VERTEX_INPUT_FORMAT_CLASS.set(conf, HiveVertexInputFormat.class);
        JythonJob.VertexInput vertexInput = vertexInputs.get(0);
        checkVertexInput(vertexInput);
        LOG.info("Setting vertex input using: " + vertexInput);

        HIVE_VERTEX_INPUT.getDatabaseOpt().set(conf,
            jythonJob.getHive_database());
        HIVE_VERTEX_INPUT.getTableOpt().set(conf, vertexInput.getTable());
        if (vertexInput.getPartition_filter() != null) {
          HIVE_VERTEX_INPUT.getPartitionOpt().set(conf,
              vertexInput.getPartition_filter());
        }
        HIVE_VERTEX_INPUT.getClassOpt().set(conf, JythonHiveToVertex.class);

        JythonHiveToVertex.VERTEX_ID_COLUMN.set(conf,
            vertexInput.getId_column());
        if (vertexInput.getValue_column() != null) {
          JythonHiveToVertex.VERTEX_VALUE_COLUMN.set(conf,
              vertexInput.getValue_column());
        }
      } else {
        VERTEX_INPUT_FORMAT_CLASS.set(conf, MultiVertexInputFormat.class);
        throw new IllegalArgumentException(
            "Multiple vertex inputs not supported yet: " + vertexInputs);
      }
    }
  }

  /**
   * Check that the vertex input info is valid
   *
   * @param vertexInput data to check
   */
  private static void checkVertexInput(JythonJob.VertexInput vertexInput) {
    checkNotNull(vertexInput.getTable(),
        "VertexInput table name needs to be set");
    checkNotNull(vertexInput.getId_column(),
        "VertexInput ID column needs to be set");
  }

  /**
   * Check if the writable is holding a primitive type
   *
   * @param klass Writable class
   * @return true if writable is holding primitive
   */
  public static boolean isPrimitiveWritable(Class klass) {
    return NullWritable.class.equals(klass) ||
        BooleanWritable.class.equals(klass) ||
        ByteWritable.class.equals(klass) ||
        IntWritable.class.equals(klass) ||
        LongWritable.class.equals(klass) ||
        FloatWritable.class.equals(klass) ||
        DoubleWritable.class.equals(klass);
  }

  /**
   * Tell whether the user type given is a primitive writable
   *
   * @param typeHolder TypeHolder
   * @return true if type is a Java primitive writable
   */
  public static boolean userTypeIsJavaPrimitiveWritable(
      JythonJob.TypeHolder typeHolder) {
    Object type = typeHolder.getType();
    if (type instanceof Class) {
      return isPrimitiveWritable((Class) type);
    } else if (type instanceof String) {
      try {
        Class klass = Class.forName((String) type);
        return isPrimitiveWritable(klass);
      } catch (ClassNotFoundException e) {
        return false;
      }
    } else {
      return false;
    }
  }

  /**
   * Initialize output info
   *
   * @param conf Configuration
   * @param jythonJob the job info
   */
  private static void initOutput(Configuration conf, JythonJob jythonJob) {
    JythonJob.VertexOutput vertexOutput = jythonJob.getVertex_output();
    if (vertexOutput.getTable() != null) {
      LOG.info("Setting vertex output using: " + vertexOutput);
      VERTEX_OUTPUT_FORMAT_CLASS.set(conf, HiveVertexOutputFormat.class);
      VERTEX_TO_HIVE_CLASS.set(conf, JythonVertexToHive.class);
      JythonVertexToHive.VERTEX_ID_COLUMN.set(conf,
          vertexOutput.getId_column());
      VERTEX_VALUE_COLUMN.set(conf, vertexOutput.getValue_column());
      HIVE_VERTEX_OUTPUT_DATABASE.set(conf, jythonJob.getHive_database());
      HIVE_VERTEX_OUTPUT_PROFILE_ID.set(conf, "vertex_output_profile");
      HIVE_VERTEX_OUTPUT_TABLE.set(conf, vertexOutput.getTable());
      if (vertexOutput.getPartition() != null) {
        HIVE_VERTEX_OUTPUT_PARTITION.set(conf,
            makePartitionString(vertexOutput.getPartition()));
      }
    }
  }

  /**
   * Create partition string
   *
   * @param parts partition pieces
   * @return partition string
   */
  private static String makePartitionString(Map<String, String> parts) {
    return Joiner.on(",").withKeyValueSeparator("=").join(parts);
  }
}
