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

package org.apache.giraph.hive.common;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.hive.input.mapping.HiveToMapping;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.output.VertexToHive;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.lang.System.getenv;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_MAPPING_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.VERTEX_TO_HIVE_CLASS;

/**
 * Utility methods for Hive IO
 */
@SuppressWarnings("unchecked")
public class HiveUtils {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(HiveUtils.class);

  /** Do not instantiate */
  private HiveUtils() {
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
   * Lookup index of column in {@link HiveTableSchema}, or throw if not found.
   *
   * @param schema {@link HiveTableSchema}
   * @param columnName column name
   * @return column index
   */
  public static int columnIndexOrThrow(HiveTableSchema schema,
      String columnName) {
    int index = schema.positionOf(columnName);
    if (index == -1) {
      throw new IllegalArgumentException("Column " + columnName +
          " not found in table " + schema.getTableDesc());
    }
    return index;
  }

  /**
   * Lookup index of column in {@link HiveTableSchema}, or throw if not found.
   *
   * @param schema {@link HiveTableSchema}
   * @param conf {@link Configuration}
   * @param confOption {@link StrConfOption}
   * @return column index
   */
  public static int columnIndexOrThrow(HiveTableSchema schema,
      Configuration conf, StrConfOption confOption) {
    String columnName = confOption.get(conf);
    if (columnName == null) {
      throw new IllegalArgumentException("Column " + confOption.getKey() +
          " not set in configuration");
    }
    return columnIndexOrThrow(schema, columnName);
  }

  /**
   * Add hive-site.xml file to tmpfiles in Configuration.
   *
   * @param conf Configuration
   */
  public static void addHiveSiteXmlToTmpFiles(Configuration conf) {
    // When output partitions are used, workers register them to the
    // metastore at cleanup stage, and on HiveConf's initialization, it
    // looks for hive-site.xml.
    addToHiveFromClassLoader(conf, "hive-site.xml");
  }

  /**
   * Add hive-site-custom.xml to tmpfiles in Configuration.
   *
   * @param conf Configuration
   */
  public static void addHiveSiteCustomXmlToTmpFiles(Configuration conf) {
    addToHiveFromClassLoader(conf, "hive-site-custom.xml");
    addToHiveFromEnv(conf, "HIVE_HOME", "conf/hive-site.xml");
  }

  /**
   * Add a file to Configuration tmpfiles from environment variable
   *
   * @param conf Configuration
   * @param envKey environment variable key
   * @param path search path
   * @return true if file found and added, false otherwise
   */
  private static boolean addToHiveFromEnv(Configuration conf,
      String envKey, String path) {
    String envValue = getenv(envKey);
    if (envValue == null) {
      return false;
    }
    File file = new File(envValue, path);
    if (file.exists()) {
      LOG.info("addToHiveFromEnv: Adding " + file.getPath() +
          " to Configuration tmpfiles");
    }
    try {
      addToStringCollection(conf, "tmpfiles", file.toURI().toURL().toString());
    } catch (MalformedURLException e) {
      LOG.error("Failed to get URL for file " + file);
    }
    return true;
  }

  /**
   * Add a file to Configuration tmpfiles from ClassLoader resource
   *
   * @param conf Configuration
   * @param name file name
   * @return true if file found in class loader, false otherwise
   */
  private static boolean addToHiveFromClassLoader(Configuration conf,
      String name) {
    URL url = conf.getClassLoader().getResource(name);
    if (url == null) {
      return false;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("addToHiveFromClassLoader: Adding " + name + " at " +
          url + " to Configuration tmpfiles");
    }
    addToStringCollection(conf, "tmpfiles", url.toString());
    return true;
  }

  /**
   * Add jars from HADOOP_CLASSPATH environment variable to tmpjars property
   * in Configuration.
   *
   * @param conf Configuration
   */
  public static void addHadoopClasspathToTmpJars(Configuration conf) {
    // Or, more effectively, we can provide all the jars client needed to
    // the workers as well
    String hadoopClasspath = getenv("HADOOP_CLASSPATH");
    if (hadoopClasspath == null) {
      return;
    }
    String[] hadoopJars = hadoopClasspath.split(File.pathSeparator);
    if (hadoopJars.length > 0) {
      List<String> hadoopJarURLs = Lists.newArrayList();
      for (String jarPath : hadoopJars) {
        File file = new File(jarPath);
        if (file.exists() && file.isFile()) {
          hadoopJarURLs.add(file.toURI().toString());
        }
      }
      HiveUtils.addToStringCollection(conf, "tmpjars", hadoopJarURLs);
    }
  }

  /**
   * Handle -hiveconf options, adding them to Configuration
   *
   * @param hiveconfArgs array of hiveconf args
   * @param conf Configuration
   */
  public static void processHiveconfOptions(String[] hiveconfArgs,
      Configuration conf) {
    for (String hiveconf : hiveconfArgs) {
      processHiveconfOption(conf, hiveconf);
    }
  }

  /**
   * Process -hiveconf option, adding it to Configuration appropriately.
   *
   * @param conf Configuration
   * @param hiveconf option to process
   */
  public static void processHiveconfOption(Configuration conf,
      String hiveconf) {
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

  /**
   * Add string to collection
   *
   * @param conf Configuration
   * @param key key to add
   * @param values values for collection
   */
  public static void addToStringCollection(Configuration conf, String key,
      String... values) {
    addToStringCollection(conf, key, Arrays.asList(values));
  }

  /**
   * Add string to collection
   *
   * @param conf Configuration
   * @param key to add
   * @param values values for collection
   */
  public static void addToStringCollection(
      Configuration conf, String key, Collection<String> values) {
    Collection<String> strings = conf.getStringCollection(key);
    strings.addAll(values);
    conf.setStrings(key, strings.toArray(new String[strings.size()]));
  }

  /**
   * Create a new VertexToHive
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf Configuration
   * @param schema Hive table schema
   * @return VertexToHive
   * @throws IOException on any instantiation errors
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable> VertexToHive<I, V, E> newVertexToHive(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      HiveTableSchema schema) throws IOException {
    Class<? extends VertexToHive> klass = VERTEX_TO_HIVE_CLASS.get(conf);
    if (klass == null) {
      throw new IOException(VERTEX_TO_HIVE_CLASS.getKey() +
          " not set in conf");
    }
    return newInstance(klass, conf, schema);
  }

  /**
   * Create a new HiveToEdge
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf Configuration
   * @param schema Hive table schema
   * @return HiveToVertex
   */
  public static <I extends WritableComparable, V extends Writable,
        E extends Writable> HiveToEdge<I, E> newHiveToEdge(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      HiveTableSchema schema) {
    Class<? extends HiveToEdge> klass = HIVE_EDGE_INPUT.getClass(conf);
    if (klass == null) {
      throw new IllegalArgumentException(
          HIVE_EDGE_INPUT.getClassOpt().getKey() + " not set in conf");
    }
    return newInstance(klass, conf, schema);
  }

  /**
   * Create a new HiveToVertex
   *
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param conf Configuration
   * @param schema Hive table schema
   * @return HiveToVertex
   */
  public static <I extends WritableComparable, V extends Writable,
        E extends Writable> HiveToVertex<I, V, E> newHiveToVertex(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      HiveTableSchema schema) {
    Class<? extends HiveToVertex> klass = HIVE_VERTEX_INPUT.getClass(conf);
    if (klass == null) {
      throw new IllegalArgumentException(
          HIVE_VERTEX_INPUT.getClassOpt().getKey() + " not set in conf");
    }
    return newInstance(klass, conf, schema);
  }

  /**
   * Create a new HiveToMapping
   *
   * @param conf ImmutableClassesGiraphConfiguration
   * @param schema HiveTableSchema
   * @param <I> vertexId type
   * @param <V> vertexValue type
   * @param <E> edgeValue type
   * @param <B> mappingTarget type
   * @return HiveToMapping
   */
  public static <I extends WritableComparable, V extends Writable,
    E extends Writable, B extends Writable>
  HiveToMapping<I, B> newHiveToMapping(
    ImmutableClassesGiraphConfiguration<I, V, E> conf,
    HiveTableSchema schema) {
    Class<? extends HiveToMapping> klass = HIVE_MAPPING_INPUT.getClass(conf);
    if (klass == null) {
      throw new IllegalArgumentException(
          HIVE_MAPPING_INPUT.getClassOpt().getKey() + " not set in conf"
      );
    }
    return newInstance(klass, conf, schema);
  }

  /**
   * Create a new instance of a class, configuring it and setting the Hive table
   * schema if it supports those types.
   *
   * @param klass Class to create
   * @param conf {@link ImmutableClassesGiraphConfiguration} to configure with
   * @param schema {@link HiveTableSchema} from Hive to set
   * @param <I> Vertex ID
   * @param <V> Vertex Value
   * @param <E> Edge Value
   * @param <T> type being created
   * @return new object of type <T>
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable, T>
  T newInstance(Class<T> klass,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      HiveTableSchema schema) {
    T object = ReflectionUtils.<T>newInstance(klass, conf);
    HiveTableSchemas.configure(object, schema);
    return object;
  }
}
