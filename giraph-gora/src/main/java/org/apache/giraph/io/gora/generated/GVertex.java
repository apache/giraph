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

package org.apache.giraph.io.gora.generated;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.util.Utf8;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;

/**
 * Example class for defining a Giraph-Vertex.
 */
@SuppressWarnings("all")
public class GVertex extends PersistentBase {
  /**
   * Schema used for the class.
   */
  public static final Schema OBJ_SCHEMA = Schema.parse(
      "{\"type\":\"record\",\"name\":\"Vertex\"," +
      "\"namespace\":\"org.apache.giraph.gora.generated\"," +
      "\"fields\":[{\"name\":\"vertexId\",\"type\":\"string\"}," +
      "{\"name\":\"value\",\"type\":\"float\"},{\"name\":\"edges\"," +
      "\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}");

  /**
   * Field enum
   */
  public static enum Field {
    /**
     * VertexId
     */
    VERTEX_ID(0, "vertexId"),

    /**
     * Field value
     */
    VALUE(1, "value"),

    /**
     * Edges
     */
    EDGES(2, "edges");

    /**
     * Field index
     */
    private int index;

    /**
     * Field name
     */
    private String name;

    /**
     * Field constructor
     * @param index of attribute
     * @param name of attribute
     */
    Field(int index, String name) {
      this.index = index;
      this.name = name;
    }

    /**
     * Gets index
     * @return int of attribute.
     */
    public int getIndex() {
      return index;
    }

    /**
     * Gets name
     * @return String of name.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets name
     * @return String of name.
     */
    public String toString() {
      return name;
    }
  };

  /**
   * Array containing all fields/
   */
  private static final String[] ALL_FIELDS = {
    "vertexId", "value", "edges"
  };

  static {
    PersistentBase.registerFields(GVertex.class, ALL_FIELDS);
  }

  /**
   * Vertex Id
   */
  private Utf8 vertexId;

  /**
   * Value
   */
  private float value;

  /**
   * Edges
   */
  private Map<Utf8, Utf8> edges;

  /**
   * Default constructor
   */
  public GVertex() {
    this(new StateManagerImpl());
  }

  /**
   * Constructor
   * @param stateManager from which the object will be created.
   */
  public GVertex(StateManager stateManager) {
    super(stateManager);
    edges = new StatefulHashMap<Utf8, Utf8>();
  }

  /**
   * Creates a new instance
   * @param stateManager from which the object will be created.
   * @return GVertex created
   */
  public GVertex newInstance(StateManager stateManager) {
    return new GVertex(stateManager);
  }

  /**
   * Gets the object schema
   * @return Schema of the object.
   */
  public Schema getSchema() {
    return OBJ_SCHEMA;
  }

  /**
   * Gets field
   * @param fieldIndex index of field to be used.
   * @return Object from an index.
   */
  public Object get(int fieldIndex) {
    switch (fieldIndex) {
    case 0:
      return vertexId;
    case 1:
      return value;
    case 2:
      return edges;
    default:
      throw new AvroRuntimeException("Bad index");
    }
  }

  /**
   * Puts a value into a field.
   * @param fieldIndex index of field used.
   * @param fieldValue value of field used.
   */
  @SuppressWarnings(value = "unchecked")
  public void put(int fieldIndex, Object fieldValue) {
    if (isFieldEqual(fieldIndex, fieldValue)) {
      return;
    }
    getStateManager().setDirty(this, fieldIndex);
    switch (fieldIndex) {
    case 0:
      vertexId = (Utf8) fieldValue; break;
    case 1:
      value = (Float) fieldValue; break;
    case 2:
      edges = (Map<Utf8, Utf8>) fieldValue; break;
    default:
      throw new AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets vertexId
   * @return Utf8 vertexId
   */
  public Utf8 getVertexId() {
    return (Utf8) get(0);
  }

  /**
   * Sets vertexId
   * @param value vertexId
   */
  public void setVertexId(Utf8 value) {
    put(0, value);
  }

  /**
   * Gets value
   * @return String of value.
   */
  public float getValue() {
    return (Float) get(1);
  }

  /**
   * Sets value
   * @param value .
   */
  public void setValue(float value) {
    put(1, value);
  }

  /**
   * Get edges.
   * @return Map of edges.
   */
  public Map<Utf8, Utf8> getEdges() {
    return (Map<Utf8, Utf8>) get(2);
  }

  /**
   * Gets value from edge.
   * @param key Edge key.
   * @return Utf8 containing the value of edge.
   */
  public Utf8 getFromEdges(Utf8 key) {
    if (edges == null) { return null; }
    return edges.get(key);
  }

  /**
   * Puts a new edge.
   * @param key of new edge.
   * @param value of new edge.
   */
  public void putToEdges(Utf8 key, Utf8 value) {
    getStateManager().setDirty(this, 2);
    edges.put(key, value);
  }

  /**
   * Remove from edges
   * @param key of edge to be deleted.
   * @return Utf8 containing value of deleted key.
   */
  public Utf8 removeFromEdges(Utf8 key) {
    if (edges == null) { return null; }
    getStateManager().setDirty(this, 2);
    return edges.remove(key);
  }
}
