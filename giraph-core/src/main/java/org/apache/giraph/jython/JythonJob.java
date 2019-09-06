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
package org.apache.giraph.jython;

import com.google.common.base.MoreObjects;
import org.apache.giraph.combiner.MessageCombiner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

// CHECKSTYLE: stop ParameterNameCheck
// CHECKSTYLE: stop MemberNameCheck
// CHECKSTYLE: stop MethodNameCheck

/**
 * Holder of Jython job information.
 */
public class JythonJob {
  /**
   * Base class for input information
   */
  public static class InputBase {
    /** Name of Hive table */
    private String table;
    /** Filter for partitions to read */
    private String partition_filter;

    public String getPartition_filter() {
      return partition_filter;
    }

    public void setPartition_filter(String partition_filter) {
      this.partition_filter = partition_filter;
    }

    public String getTable() {
      return table;
    }

    public void setTable(String table) {
      this.table = table;
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("table", table)
          .add("partition_filter", partition_filter)
          .toString();
    }
  }

  /**
   * Info about vertex input
   */
  public static class VertexInput extends InputBase {
    /** Name of vertex ID column */
    private String id_column;
    /** Name of vertex value column */
    private String value_column;

    public String getId_column() {
      return id_column;
    }

    public void setId_column(String id_column) {
      this.id_column = id_column;
    }

    public String getValue_column() {
      return value_column;
    }

    public void setValue_column(String value_column) {
      this.value_column = value_column;
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("table", getTable())
          .add("partition_filter", getPartition_filter())
          .add("id_column", id_column)
          .add("value_column", value_column)
          .toString();
    }
  }

  /**
   * Info about edge input
   */
  public static class EdgeInput extends InputBase {
    /** Name for source ID column */
    private String source_id_column;
    /** Name for target ID column */
    private String target_id_column;
    /** Name of edge value column */
    private String value_column;

    public String getValue_column() {
      return value_column;
    }

    public void setValue_column(String value_column) {
      this.value_column = value_column;
    }

    public String getSource_id_column() {
      return source_id_column;
    }

    public void setSource_id_column(String source_id_column) {
      this.source_id_column = source_id_column;
    }

    public String getTarget_id_column() {
      return target_id_column;
    }

    public void setTarget_id_column(String target_id_column) {
      this.target_id_column = target_id_column;
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("table", getTable())
          .add("partition_filter", getPartition_filter())
          .add("source_id_column", source_id_column)
          .add("target_id_column", target_id_column)
          .add("edge_value_column", value_column)
          .toString();
    }
  }

  /**
   * Info about vertex output
   */
  public static class VertexOutput {
    /** Name of hive table */
    private String table;
    /** Partition data */
    private final Map<String, String> partition = Maps.newHashMap();
    /** Name of Vertex ID column */
    private String id_column;
    /** Name of Vertex value column */
    private String value_column;

    public String getId_column() {
      return id_column;
    }

    public void setId_column(String id_column) {
      this.id_column = id_column;
    }

    public Map<String, String> getPartition() {
      return partition;
    }

    public String getTable() {
      return table;
    }

    public void setTable(String table) {
      this.table = table;
    }

    public String getValue_column() {
      return value_column;
    }

    public void setValue_column(String value_column) {
      this.value_column = value_column;
    }

    @Override public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("table", table)
          .add("partition", partition)
          .add("id_column", id_column)
          .add("value_column", value_column)
          .toString();
    }
  }

  /**
   * Holds a Java or Jython type.
   */
  public static class TypeHolder {
    /**
     * The Java or Jython class. Should be one of the following:
     *
     * 1) Name (string) of Jython class
     * 2) Java Writable class
     * 3) Name (string) of Java Writable class
     * 4) Jython class directly. This is a bit problematic, it is preferred to
     *    use the string name of the Jython class instead.
     */
    private Object type;

    public Object getType() {
      return type;
    }

    public void setType(Object type) {
      this.type = type;
    }
  }

  /**
   * A type along with its Hive input/output class.
   */
  public static class TypeWithHive extends TypeHolder {
    /** Hive Reader */
    private Object hive_reader;
    /** Hive Writer */
    private Object hive_writer;
    /** Hive Reader and Writer in one */
    private Object hive_io;

    public Object getHive_io() {
      return hive_io;
    }

    public void setHive_io(Object hive_io) {
      this.hive_io = hive_io;
    }

    public Object getHive_reader() {
      return hive_reader;
    }

    public void setHive_reader(Object hive_reader) {
      this.hive_reader = hive_reader;
    }

    public Object getHive_writer() {
      return hive_writer;
    }

    public void setHive_writer(Object hive_writer) {
      this.hive_writer = hive_writer;
    }
  }

  /** Name of job */
  private String name;
  /** Hive database */
  private String hive_database = "digraph";
  /** Number of workers */
  private int workers;
  /** MapReduce pool */
  private String pool;
  /** Vertex ID */
  private final TypeWithHive vertex_id = new TypeWithHive();
  /** Vertex value */
  private final TypeWithHive vertex_value = new TypeWithHive();
  /** Edge value */
  private final TypeWithHive edge_value = new TypeWithHive();
  /** Incoming message value */
  private final TypeHolder incoming_message_value = new TypeHolder();
  /** Outgoing message value */
  private final TypeHolder outgoing_message_value = new TypeHolder();
  /** Message value type - used to set both in/out message types in one call. */
  private final TypeHolder message_value = new TypeHolder();
  /** Computation class */
  private String computation_name;
  /** MessageCombiner class */
  private Class<? extends MessageCombiner> messageCombiner;
  /** Java options */
  private final List<String> java_options = Lists.newArrayList();
  /** Giraph options */
  private final Map<String, Object> giraph_options = Maps.newHashMap();
  /** Vertex inputs */
  private final List<VertexInput> vertex_inputs = Lists.newArrayList();
  /** Edge inputs */
  private final List<EdgeInput> edge_inputs = Lists.newArrayList();
  /** Output */
  private final VertexOutput vertex_output = new VertexOutput();

  /////// Read only info for jython scripts ////////
  /** User running the job */
  private String user;

  /**
   * Constructor
   */
  public JythonJob() {
    user = System.getProperty("user.name");
    workers = 5;
  }

  public String getUser() {
    return user;
  }

  public TypeWithHive getVertex_value() {
    return vertex_value;
  }

  public TypeWithHive getVertex_id() {
    return vertex_id;
  }

  public TypeWithHive getEdge_value() {
    return edge_value;
  }

  public TypeHolder getIncoming_message_value() {
    return incoming_message_value;
  }

  public TypeHolder getOutgoing_message_value() {
    return outgoing_message_value;
  }

  public TypeHolder getMessage_value() {
    return message_value;
  }

  public List<String> getJava_options() {
    return java_options;
  }

  public Map<String, Object> getGiraph_options() {
    return giraph_options;
  }

  public Class<? extends MessageCombiner> getMessageCombiner() {
    return messageCombiner;
  }

  public void setMessageCombiner(
      Class<? extends MessageCombiner> messageCombiner) {
    this.messageCombiner = messageCombiner;
  }

  public String getComputation_name() {
    return computation_name;
  }

  public void setComputation_name(String computation_name) {
    this.computation_name = computation_name;
  }

  public String getHive_database() {
    return hive_database;
  }

  public void setHive_database(String hive_database) {
    this.hive_database = hive_database;
  }

  public List<EdgeInput> getEdge_inputs() {
    return edge_inputs;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPool() {
    return pool;
  }

  public void setPool(String pool) {
    this.pool = pool;
  }

  public List<VertexInput> getVertex_inputs() {
    return vertex_inputs;
  }

  public VertexOutput getVertex_output() {
    return vertex_output;
  }

  public int getWorkers() {
    return workers;
  }

  public void setWorkers(int workers) {
    this.workers = workers;
  }
}

// CHECKSTYLE: resume ParameterNameCheck
// CHECKSTYLE: resume MemberNameCheck
// CHECKSTYLE: resume MethodNameCheck
