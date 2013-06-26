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

import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.hadoop.conf.Configuration;

import com.facebook.hiveio.input.HiveInputDescription;

/**
 * Holder for Hive Input Configuration options. Used for vertex and edge input.
 * @param <C> {@link org.apache.giraph.hive.input.edge.HiveToEdge} or
 *            {@link org.apache.giraph.hive.input.vertex.HiveToVertex}
 */
public class HiveInputOptions<C> {
  /** Class for converting hive records */
  private final ClassConfOption<C> classOpt;
  /** Input profile id */
  private final StrConfOption profileIdOpt;
  /** Number of splits */
  private final IntConfOption splitsOpt;
  /** Input database name */
  private final StrConfOption databaseOpt;
  /** Input table name */
  private final StrConfOption tableOpt;
  /** Input partition filter */
  private final StrConfOption partitionOpt;
  /** Hive Metastore host to use. If blank will infer from HiveConf */
  private final StrConfOption hostOpt;
  /** Hive Metastore port to use. */
  private final IntConfOption portOpt;

  /**
   * Constructor
   * @param name "vertex" or "edge"
   * @param hiveToTypeClass HiveToVertex or HiveToEdge
   */
  public HiveInputOptions(String name, Class<C> hiveToTypeClass) {
    classOpt = ClassConfOption.<C>create(key(name, "class"),
        null, hiveToTypeClass, "Class for converting hive records");
    profileIdOpt = new StrConfOption(key(name, "profileId"),
        name + "_input_profile", "Input profile id");
    partitionOpt = new StrConfOption(key(name, "partition"), "",
        "Input partition filter");
    splitsOpt = new IntConfOption(key(name, "splits"), 0, "Number of splits");
    databaseOpt = new StrConfOption(key(name, "database"), "default",
        "Input database name");
    tableOpt = new StrConfOption(key(name, "table"), "", "Input table name");
    hostOpt = new StrConfOption(key(name, "metastore.host"), null,
        "Hive Metastore host to use. If blank will infer from HiveConf");
    portOpt = new IntConfOption(key(name, "metastore.port"), 9083,
        "Hive Metastore port to use.");
  }

  /**
   * Create Configuration key from name and suffix
   * @param name the name
   * @param suffix the suffix
   * @return key
   */
  private static String key(String name, String suffix) {
    return "giraph.hive.input." + name + "." + suffix;
  }

  /**
   * Get profile ID from Configuration
   * @param conf Configuration
   * @return profile ID
   */
  public String getProfileID(Configuration conf) {
    return profileIdOpt.get(conf);
  }

  /**
   * Set HiveToX class to use
   * @param conf Configuraton
   * @param hiveToTypeClass class to use
   */
  public void setClass(Configuration conf, Class<? extends C> hiveToTypeClass) {
    classOpt.set(conf, hiveToTypeClass);
  }

  /**
   * Set Database to use
   * @param conf Configuration
   * @param dbName database
   */
  public void setDatabase(Configuration conf, String dbName) {
    databaseOpt.set(conf, dbName);
  }

  /**
   * Set Table to use
   * @param conf Configuration
   * @param tableName table
   */
  public void setTable(Configuration conf, String tableName) {
    tableOpt.set(conf, tableName);
  }

  /**
   * Set partition filter to use
   * @param conf Configuration
   * @param partitionFilter partition filter
   */
  public void setPartition(Configuration conf, String partitionFilter) {
    partitionOpt.set(conf, partitionFilter);
  }

  /**
   * Get HiveToX class set in Configuration
   * @param conf Configuration
   * @return HiveToX
   */
  public Class<? extends C> getClass(Configuration conf) {
    return classOpt.get(conf);
  }

  public StrConfOption getDatabaseOpt() {
    return databaseOpt;
  }

  public StrConfOption getHostOpt() {
    return hostOpt;
  }

  public ClassConfOption<C> getClassOpt() {
    return classOpt;
  }

  public StrConfOption getPartitionOpt() {
    return partitionOpt;
  }

  public IntConfOption getPortOpt() {
    return portOpt;
  }

  public StrConfOption getProfileIdOpt() {
    return profileIdOpt;
  }

  public IntConfOption getSplitsOpt() {
    return splitsOpt;
  }

  public StrConfOption getTableOpt() {
    return tableOpt;
  }

  /**
   * Create a HiveInputDescription from the options in the Configuration
   * @param conf Configuration
   * @return HiveInputDescription
   */
  public HiveInputDescription makeInputDescription(Configuration conf) {
    HiveInputDescription inputDescription = new HiveInputDescription();
    inputDescription.getTableDesc().setDatabaseName(databaseOpt.get(conf));
    inputDescription.getTableDesc().setTableName(tableOpt.get(conf));
    inputDescription.setPartitionFilter(partitionOpt.get(conf));
    inputDescription.setNumSplits(splitsOpt.get(conf));
    inputDescription.getMetastoreDesc().setHost(hostOpt.get(conf));
    inputDescription.getMetastoreDesc().setPort(portOpt.get(conf));
    return inputDescription;
  }
}
