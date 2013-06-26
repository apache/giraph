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
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.output.VertexToHive;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TException;

import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.output.HiveApiOutputFormat;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.HiveTableSchemas;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT;
import static org.apache.giraph.hive.common.GiraphHiveConstants.VERTEX_TO_HIVE_CLASS;
import static org.apache.giraph.utils.ReflectionUtils.newInstance;

/**
 * Utility methods for Hive IO
 */
public class HiveUtils {
  /** Do not instantiate */
  private HiveUtils() {
  }

  /**
   * Initialize hive input, prepare Configuration parameters
   *
   * @param hiveInputFormat HiveApiInputFormat
   * @param inputDescription HiveInputDescription
   * @param profileId profile ID
   * @param conf Configuration
   */
  public static void initializeHiveInput(HiveApiInputFormat hiveInputFormat,
      HiveInputDescription inputDescription, String profileId,
      Configuration conf) {
    hiveInputFormat.setMyProfileId(profileId);
    HiveApiInputFormat.setProfileInputDesc(conf, inputDescription, profileId);
    HiveTableSchema schema = HiveTableSchemas.lookup(conf,
        inputDescription.getTableDesc());
    HiveTableSchemas.put(conf, profileId, schema);
  }

  /**
   * Initialize hive output, prepare Configuration parameters
   *
   * @param hiveOutputFormat HiveApiOutputFormat
   * @param outputDesc HiveOutputDescription
   * @param profileId Profile id
   * @param conf Configuration
   */
  public static void initializeHiveOutput(HiveApiOutputFormat hiveOutputFormat,
      HiveOutputDescription outputDesc, String profileId, Configuration conf) {
    hiveOutputFormat.setMyProfileId(profileId);
    try {
      HiveApiOutputFormat.initProfile(conf, outputDesc, profileId);
    } catch (TException e) {
      throw new IllegalStateException(
          "initializeHiveOutput: TException occurred", e);
    }
    HiveTableSchema schema = HiveTableSchemas.lookup(conf,
        outputDesc.getTableDesc());
    HiveTableSchemas.put(conf, profileId, schema);
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
    VertexToHive<I, V, E> vertexToHive = newInstance(klass, conf);
    HiveTableSchemas.configure(vertexToHive, schema);
    return vertexToHive;
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
    HiveToEdge hiveToEdge = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToEdge, schema);
    return hiveToEdge;
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
    HiveToVertex hiveToVertex = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToVertex, schema);
    return hiveToVertex;
  }
}
