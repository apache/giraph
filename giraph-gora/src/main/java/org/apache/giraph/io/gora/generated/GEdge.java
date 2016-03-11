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

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.util.Utf8;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.impl.StateManagerImpl;

/**
 * Example class for defining a Giraph-Edge.
 */
@SuppressWarnings("all")
public class GEdge extends PersistentBase {
  /**
   * Schema used for the class.
   */
  public static final Schema OBJ_SCHEMA = Schema.parse("{\"type\":\"record\"," +
    "\"name\":\"GEdge\",\"namespace\":\"org.apache.giraph.gora.generated\"," +
    "\"fields\":[{\"name\":\"edgeId\",\"type\":\"string\"}," +
    "{\"name\":\"edgeWeight\",\"type\":\"float\"}," +
    "{\"name\":\"vertexInId\",\"type\":\"string\"}," +
    "{\"name\":\"vertexOutId\",\"type\":\"string\"}," +
    "{\"name\":\"label\",\"type\":\"string\"}]}");

  /**
   * Field enum
   */
  public static enum Field {
    /**
     * Edge id.
     */
    EDGE_ID(0, "edgeId"),

    /**
     * Edge weight.
     */
    EDGE_WEIGHT(1, "edgeWeight"),

    /**
     * Edge vertex source id.
     */
    VERTEX_IN_ID(2, "vertexInId"),

    /**
     * Edge vertex end id.
     */
    VERTEX_OUT_ID(3, "vertexOutId"),

    /**
     * Edge label.
     */
    LABEL(4, "label");

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
    "edgeId", "edgeWeight", "vertexInId", "vertexOutId", "label"
  };

  static {
    PersistentBase.registerFields(GEdge.class, ALL_FIELDS);
  }

  /**
   * edgeId
   */
  private Utf8 edgeId;

  /**
   * edgeWeight
   */
  private float edgeWeight;

  /**
   * vertexInId
   */
  private Utf8 vertexInId;

  /**
   * vertexOutId
   */
  private Utf8 vertexOutId;

  /**
   * label
   */
  private Utf8 label;

  /**
   * Default constructor.
   */
  public GEdge() {
    this(new StateManagerImpl());
  }

  /**
   * Constructor
   * @param stateManager from which the object will be created.
   */
  public GEdge(StateManager stateManager) {
    super(stateManager);
  }

  /**
   * Creates a new instance
   * @param stateManager from which the object will be created.
   * @return GEdge created
   */
  public GEdge newInstance(StateManager stateManager) {
    return new GEdge(stateManager);
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
   * @param fieldIndex index field.
   * @return Object from an index.
   */
  public Object get(int fieldIndex) {
    switch (fieldIndex) {
    case 0:
      return edgeId;
    case 1:
      return edgeWeight;
    case 2:
      return vertexInId;
    case 3:
      return vertexOutId;
    case 4:
      return label;
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
      edgeId = (Utf8) fieldValue; break;
    case 1:
      edgeWeight = (Float) fieldValue; break;
    case 2:
      vertexInId = (Utf8) fieldValue; break;
    case 3:
      vertexOutId = (Utf8) fieldValue; break;
    case 4:
      label = (Utf8) fieldValue; break;
    default:
      throw new AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets edgeId
   * @return Utf8 edgeId
   */
  public Utf8 getEdgeId() {
    return (Utf8) get(0);
  }

  /**
   * Sets edgeId
   * @param value edgeId
   */
  public void setEdgeId(Utf8 value) {
    put(0, value);
  }

  /**
   * Gets edgeWeight
   * @return float edgeWeight
   */
  public float getEdgeWeight() {
    return (Float) get(1);
  }

  /**
   * Sets edgeWeight
   * @param value edgeWeight
   */
  public void setEdgeWeight(float value) {
    put(1, value);
  }

  /**
   * Gets edgeVertexInId
   * @return Utf8 edgeVertexInId
   */
  public Utf8 getVertexInId() {
    return (Utf8) get(2);
  }

  /**
   * Sets edgeVertexInId
   * @param value edgeVertexInId
   */
  public void setVertexInId(Utf8 value) {
    put(2, value);
  }

  /**
   * Gets edgeVertexOutId
   * @return Utf8 edgeVertexOutId
   */
  public Utf8 getVertexOutId() {
    return (Utf8) get(3);
  }

  /**
   * Sets edgeVertexOutId
   * @param value edgeVertexOutId
   */
  public void setVertexOutId(Utf8 value) {
    put(3, value);
  }

  /**
   * Gets edgeLabel
   * @return Utf8 edgeLabel
   */
  public Utf8 getLabel() {
    return (Utf8) get(4);
  }

  /**
   * Sets edgeLabel
   * @param value edgeLabel
   */
  public void setLabel(Utf8 value) {
    put(4, value);
  }
}
