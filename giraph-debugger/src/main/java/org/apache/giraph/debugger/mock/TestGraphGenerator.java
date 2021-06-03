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
package org.apache.giraph.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

/**
 * The code generator to generate the end-to-end test case.
 *
 * author Brian Truong Ba Quan
 */
public class TestGraphGenerator extends VelocityBasedGenerator {

  /**
   * Currently supported writables on vertices and edges or ids of vertices.
   */
  private enum WritableType {
    /**
     * {@link NullWritable}.
     */
    NULL,
    /**
     * {@link LongWritable}.
     */
    LONG,
    /**
     * {@link DoubleWritable}.
     */
    DOUBLE
  };

  /**
   * Generates an end-to-end unit test.
   * @param inputStrs a set of strings storing edge and vertex values and
   * ids of vertices.
   * @return an end-to-end unit test stored as a string (to be saved in a file.)
   */
  public String generate(String[] inputStrs) throws IOException {
    VelocityContext context = buildContext(inputStrs);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("TestGraphTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  /**
   * Builds the velocity context from the given input strings storing edge,
   * vertex values and ids of vertices.
   * @param inputStrs an array of strings storing edge and vertex values.
   * @return {@link VelocityContext} object.
   */
  private VelocityContext buildContext(String[] inputStrs) {
    VelocityContext context = new VelocityContext();
    context.put("helper", new FormatHelper());
    // Parse the string and check whether the inputs are integers or
    // floating-point numbers
    String[][] tokens = new String[inputStrs.length][];
    WritableType idWritableType = WritableType.NULL;
    WritableType valueWritableType = WritableType.NULL;
    WritableType edgeValueWritableType = WritableType.NULL;
    for (int i = 0; i < inputStrs.length; i++) {
      tokens[i] = inputStrs[i].trim().split("\\s+");
      String[] nums = tokens[i][0].split(":");
      WritableType type = parseWritableType(nums[0]);
      idWritableType = type.ordinal() > idWritableType.ordinal() ? type :
        idWritableType;
      if (nums.length > 1) {
        type = parseWritableType(nums[1]);
        valueWritableType = type.ordinal() > valueWritableType.ordinal() ?
          type : valueWritableType;
      }

      for (int j = 1; j < tokens[i].length; j++) {
        nums = tokens[i][j].split(":");
        type = parseWritableType(nums[0]);
        idWritableType = type.ordinal() > idWritableType
          .ordinal() ? type : idWritableType;
        if (nums.length > 1) {
          type = parseWritableType(nums[1]);
          edgeValueWritableType = type.ordinal() > edgeValueWritableType
            .ordinal() ? type : edgeValueWritableType;
        }
      }
    }

    Map<Object, TemplateVertex> vertexMap = new LinkedHashMap<>(
      inputStrs.length);
    String str;
    for (int i = 0; i < inputStrs.length; i++) {
      String[] nums = tokens[i][0].split(":");
      Object id = convertToSuitableType(nums[0], idWritableType);
      str = nums.length > 1 ? nums[1] : "0";
      Object value = convertToSuitableType(str, valueWritableType);
      TemplateVertex vertex = vertexMap.get(id);
      if (vertex == null) {
        vertex = new TemplateVertex(id);
        vertexMap.put(id, vertex);
      }
      vertex.setValue(value);

      for (int j = 1; j < tokens[i].length; j++) {
        nums = tokens[i][j].split(":");
        Object nbrId = convertToSuitableType(nums[0], idWritableType);
        str = nums.length > 1 ? nums[1] : "0";
        Object edgeValue = convertToSuitableType(str, edgeValueWritableType);
        if (!vertexMap.containsKey(nbrId)) {
          vertexMap.put(nbrId, new TemplateVertex(nbrId));
        }
        vertex.addNeighbor(nbrId, edgeValue);
      }
    }

    updateContextByWritableType(context, "vertexIdClass", idWritableType);
    updateContextByWritableType(context, "vertexValueClass", valueWritableType);
    updateContextByWritableType(context, "edgeValueClass",
      edgeValueWritableType);
    context.put("vertices", vertexMap);

    return context;
  }

  /**
   * Returns the {@link Writable} type of the given string value. Tries to
   * parse into different types, Long, double, and if one succeeds returns that
   * type. Otherwise returns null type.
   * @param str string containing a value.
   * @return {@link Writable} type of the given value of the string.
   */
  private WritableType parseWritableType(String str) {
    if (str == null) {
      return WritableType.NULL;
    } else {
      try {
        Long.valueOf(str);
        return WritableType.LONG;
      } catch (NumberFormatException ex) {
        return WritableType.DOUBLE;
      }
    }
  }

  /**
   * Puts the a given type of a value of an edge or a vertex or id
   * of a vertex into the context.
   * @param context {@link VelocityContext} to populate.
   * @param contextKey currently one of vertexIdClass, vertexValueClass, or
   * edgeValueClass.
   * @param type currently one of {@link NullWritable}, {@link LongWritable},
   * {@link DoubleWritable}.
   */
  private void updateContextByWritableType(VelocityContext context,
    String contextKey, WritableType type) {
    switch (type) {
    case NULL:
      context.put(contextKey, NullWritable.class.getSimpleName());
      break;
    case LONG:
      context.put(contextKey, LongWritable.class.getSimpleName());
      break;
    case DOUBLE:
      context.put(contextKey, DoubleWritable.class.getSimpleName());
      break;
    default:
      throw new IllegalStateException("Unknown type!");
    }
  }

  /**
   * Constructs a {@link Writable} object with the appropriate type that
   * contains the specified content. For example, type can be LongWritable,
   * and content can be 100L, and this method would return a new
   * {@link LongWritable} that has value 100.
   * @param contents contetns of the writable.
   * @param type type of the writable.
   * @return a {@link Writable} object of appropriate type, whose value contains
   * the given contents.
   */
  private Writable convertToSuitableType(String contents, WritableType type) {
    switch (type) {
    case NULL:
      return NullWritable.get();
    case LONG:
      return new LongWritable(Long.valueOf(contents));
    case DOUBLE:
      return new DoubleWritable(Double.valueOf(contents));
    default:
      throw new IllegalStateException("Unknown type!");
    }
  }

  /**
   * A wrapper around a simple in-memory representation of a vertex to use
   * during test generation.
   */
  public static class TemplateVertex {
    /**
     * Id of the vertex.
     */
    private final Object id;
    /**
     * Value of the vertex.
     */
    private Object value;
    /**
     * Neighbors of the vertex.
     */
    private final ArrayList<TemplateNeighbor> neighbors;

    /**
     * Constructor.
     * @param id if the vertex.
     */
    public TemplateVertex(Object id) {
      super();
      this.id = id;
      this.neighbors = new ArrayList<>();
    }

    public Object getId() {
      return id;
    }

    public Object getValue() {
      return value;
    }

    public void setValue(Object value) {
      this.value = value;
    }

    public ArrayList<TemplateNeighbor> getNeighbors() {
      return neighbors;
    }

    /**
     * Adds a neighbor to the vertex's adjacency list.
     * @param nbrId id of the neighbor.
     * @param edgeValue value on the edge to the neighbor.
     */
    public void addNeighbor(Object nbrId, Object edgeValue) {
      neighbors.add(new TemplateNeighbor(nbrId, edgeValue));
    }
  }

  /**
   * A wrapper around a simple in-memory representation of a neighbor of a
   * vertex to use during test generation.
   */

  public static class TemplateNeighbor {
    /**
     * Id of the neighbor.
     */
    private final Object id;
    /**
     * Value on the edge from vertex to the neighbor.
     */
    private final Object edgeValue;

    /**
     * Constructor.
     * @param id id of the neighbor.
     * @param edgeValue value on the edge from vertex to the neighbor.
     */
    public TemplateNeighbor(Object id, Object edgeValue) {
      super();
      this.id = id;
      this.edgeValue = edgeValue;
    }

    public Object getId() {
      return id;
    }

    public Object getEdgeValue() {
      return edgeValue;
    }
  }
}
