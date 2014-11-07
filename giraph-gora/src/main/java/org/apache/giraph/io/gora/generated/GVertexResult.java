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

/**
 * Example class for defining a Giraph-vertex result.
 */
@SuppressWarnings("all")
public class GVertexResult extends
    org.apache.gora.persistency.impl.PersistentBase implements
    org.apache.avro.specific.SpecificRecord,
    org.apache.gora.persistency.Persistent {

  /**
   * Schema used for the class.
   */
  public static final org.apache.avro.Schema SCHEMAS =
      new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\"," +
            "\"name\":\"GVertexResult\"," +
            "\"namespace\":\"org.apache.giraph.io.gora.generated\"," +
            "\"fields\":[{\"name\":\"vertexId\",\"type\":\"string\"}," +
            "{\"name\":\"vertexValue\",\"type\":\"float\"}," +
            "{\"name\":\"edges\",\"type\":" +
            "{\"type\":\"map\",\"values\":\"string\"}}]}");

  /** Enum containing all data bean's fields. */
  public static enum Field {
    /**
     * Vertex id.
     */
    VERTEX_ID(0, "vertexId"),

    /**
     * Vertex value.
     */
    VERTEX_VALUE(1, "vertexValue"),

    /**
     * Vertex edges.
     */
    EDGES(2, "edges");

    /**
     * Field's index.
     */
    private int index;

    /**
     * Field's name.
     */
    private String name;

    /**
     * Field's constructor
     * @param index field's index.
     * @param name field's name.
     */
    Field(int index, String name) {
      this.index = index;
      this.name = name;
    }

    /**
     * Gets field's index.
     * @return int field's index.
     */
    public int getIndex() {
      return index;
    }

    /**
     * Gets field's name.
     * @return String field's name.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets field's attributes to string.
     * @return String field's attributes to string.
     */
    public String toString() {
      return name;
    }
  };

  /**
   * Array containing all fields/
   */
  private static final String[] ALL_FIELDS = {
    "vertexId", "vertexValue", "edges", };

  /**
   * Tombstone.
   */
  private static final Tombstone TOMBSTONE = new Tombstone();

  /**
   * vertexId.
   */
  private java.lang.CharSequence vertexId;

  /**
   * vertexValue.
   */
  private float vertexValue;

  /**
   * edges.
   */
  private java.util.Map<java.lang.CharSequence, java.lang.CharSequence> edges;

  /**
   * Gets the total field count.
   * @return int field count
   */
  public int getFieldsCount() {
    return GVertexResult.ALL_FIELDS.length;
  }

  /**
   * Gets the schema
   * @return Schema
   */
  public org.apache.avro.Schema getSchema() {
    return SCHEMAS;
  }

  /**
   * Gets field
   * @param field index field.
   * @return Object from an index.
   */
  public java.lang.Object get(int field) {
    switch (field) {
    case 0:
      return vertexId;
    case 1:
      return vertexValue;
    case 2:
      return edges;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Puts a value into a field.
   * @param field index of field used.
   * @param value value of field used.
   */
  @SuppressWarnings(value = "unchecked")
  public void put(int field, java.lang.Object value) {
    switch (field) {
    case 0:
      vertexId = (java.lang.CharSequence) value;
      break;
    case 1:
      vertexValue = (java.lang.Float) value;
      break;
    case 2:
      edges = (java.util.Map<java.lang.CharSequence, java.lang.CharSequence>)
        ((value instanceof org.apache.gora.persistency.Dirtyable) ? value :
        new org.apache.gora.persistency.impl.DirtyMapWrapper((java.util.Map)
            value));
      break;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'vertexId' field.
   * @return CharSequence
   */
  public java.lang.CharSequence getVertexId() {
    return vertexId;
  }

  /**
   * Sets the value of the 'vertexId' field.
   * @param value the value to set.
   */
  public void setVertexId(java.lang.CharSequence value) {
    this.vertexId = value;
    setDirty(0);
  }

  /**
   * Checks the dirty status of the 'vertexId' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isVertexIdDirty(java.lang.CharSequence value) {
    return isDirty(0);
  }

  /**
   * Gets the value of the 'vertexValue' field.
   * @return Float
   */
  public java.lang.Float getVertexValue() {
    return vertexValue;
  }

  /**
   * Sets the value of the 'vertexValue' field.
     * @param value the value to set.
   */
  public void setVertexValue(java.lang.Float value) {
    this.vertexValue = value;
    setDirty(1);
  }

  /**
   * Checks the dirty status of the 'vertexValue' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isVertexValueDirty(java.lang.Float value) {
    return isDirty(1);
  }

  /**
   * Gets the value of the 'edges' field.
   * @return Edges
   */
  public java.util.Map<java.lang.CharSequence, java.lang.CharSequence>
  getEdges() {
    return edges;
  }

  /**
   * Sets the value of the 'edges' field.
   * @param value the value to set.
   */
  public void setEdges(
      java.util.Map<java.lang.CharSequence, java.lang.CharSequence> value) {
    this.edges = (value instanceof org.apache.gora.persistency.Dirtyable) ?
        value : new org.apache.gora.persistency.impl.DirtyMapWrapper(value);
    setDirty(2);
  }

  /**
   * Checks the dirty status of the 'edges' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isEdgesDirty(
      java.util.Map<java.lang.CharSequence, java.lang.CharSequence> value) {
    return isDirty(2);
  }

  /**
   * Creates a new GVertexResult RecordBuilder
   * @return GVertexResult.Builder
   */
  public static org.apache.giraph.io.gora.generated.GVertexResult.Builder
  newBuilder() {
    return new org.apache.giraph.io.gora.generated.GVertexResult.Builder();
  }

  /**
   * Creates a new GVertexResult RecordBuilder by copying an existing Builder.
   * @param other GVertexResult.Builder
   * @return GVertexResult.Builder
   */
  public static org.apache.giraph.io.gora.generated.GVertexResult.Builder
  newBuilder(org.apache.giraph.io.gora.generated.GVertexResult.Builder other) {
    return new org.apache.giraph.io.gora.generated.GVertexResult.Builder(other);
  }

  /**
   * Creates a new GVertexResult RecordBuilder by copying an existing
   * GVertexResult instance
   * @param other GVertexResult
   * @return GVertexResult.Builder
   */
  public static org.apache.giraph.io.gora.generated.GVertexResult.Builder
  newBuilder(org.apache.giraph.io.gora.generated.GVertexResult other) {
    return new org.apache.giraph.io.gora.generated.GVertexResult.Builder(other);
  }

  /**
   * Makes a deep copy from a bytebuffer.
   * @param input ByteBuffer
   * @return ByteBuffer
   */
  private static java.nio.ByteBuffer deepCopyToReadOnlyBuffer(
      java.nio.ByteBuffer input) {
    java.nio.ByteBuffer copy = java.nio.ByteBuffer.allocate(input.capacity());
    int position = input.position();
    input.reset();
    int mark = input.position();
    int limit = input.limit();
    input.rewind();
    input.limit(input.capacity());
    copy.put(input);
    input.rewind();
    copy.rewind();
    input.position(mark);
    input.mark();
    copy.position(mark);
    copy.mark();
    input.position(position);
    copy.position(position);
    input.limit(limit);
    copy.limit(limit);
    return copy.asReadOnlyBuffer();
  }

  /**
   * RecordBuilder for GVertexResult instances.
   */
  public static class Builder extends
      org.apache.avro.specific.SpecificRecordBuilderBase<GVertexResult>
      implements org.apache.avro.data.RecordBuilder<GVertexResult> {

    /**
     * vertexId
     */
    private java.lang.CharSequence vertexId;

    /**
     * vertexValue
     */
    private float vertexValue;

    /**
     * edges
     */
    private java.util.Map<java.lang.CharSequence, java.lang.CharSequence> edges;

    /** Creates a new Builder */
    private Builder() {
      super(org.apache.giraph.io.gora.generated.GVertexResult.SCHEMAS);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other GVertexResult.Builder
     */
    private Builder(
        org.apache.giraph.io.gora.generated.GVertexResult.Builder other) {
      super(other);
    }

    /**
     * Creates a Builder by copying an existing GVertexResult instance.
     * @param other GVertexResult
     */
    // CHECKSTYLE: stop Indentation
    private Builder(org.apache.giraph.io.gora.generated.GVertexResult other) {
      super(org.apache.giraph.io.gora.generated.GVertexResult.SCHEMAS);
      if (isValidValue(fields()[0], other.vertexId)) {
        this.vertexId = (java.lang.CharSequence) data().deepCopy(
            fields()[0].schema(), other.vertexId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.vertexValue)) {
        this.vertexValue = (java.lang.Float) data().deepCopy(
            fields()[1].schema(), other.vertexValue);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.edges)) {
        this.edges =
            (java.util.Map<java.lang.CharSequence, java.lang.CharSequence>)
            data().deepCopy(fields()[2].schema(), other.edges);
        fieldSetFlags()[2] = true;
      }
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'vertexId' field.
     * @return CharSequence
     */
    public java.lang.CharSequence getVertexId() {
      return vertexId;
    }

    /**
     * Sets the value of the 'vertexId' field.
     * @param value CharSequence
     * @return GVertexResult.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GVertexResult.Builder
    setVertexId(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.vertexId = value;
      fieldSetFlags()[0] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'vertexId' field has been set.
     * @return boolean
     */
    public boolean hasVertexId() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'vertexId' field
     * @return GVertexResult.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GVertexResult.Builder
    clearVertexId() {
      vertexId = null;
      fieldSetFlags()[0] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'vertexValue' field.
     * @return Float
     */
    public java.lang.Float getVertexValue() {
      return vertexValue;
    }

    /**
     * Sets the value of the 'vertexValue' field.
     * @param value float
     * @return GVertexResult.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GVertexResult.Builder
    setVertexValue(float value) {
      validate(fields()[1], value);
      this.vertexValue = value;
      fieldSetFlags()[1] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'vertexValue' field has been set.
     * @return boolean
     */
    public boolean hasVertexValue() {
      return fieldSetFlags()[1];
    }

    /**
     * Clears the value of the 'vertexValue' field.
     * @return GVertexResult.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GVertexResult.Builder
    clearVertexValue() {
      fieldSetFlags()[1] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'edges' field.
     * @return java.util.Map
     */
    public java.util.Map<java.lang.CharSequence, java.lang.CharSequence>
    getEdges() {
      return edges;
    }

    /**
     * Sets the value of the 'edges' field
     * @param value java.util.Map
     * @return GVertexResult.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GVertexResult.Builder setEdges(
    java.util.Map<java.lang.CharSequence, java.lang.CharSequence> value) {
      validate(fields()[2], value);
      this.edges = value;
      fieldSetFlags()[2] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'edges' field has been set.
     * @return boolean
     */
    public boolean hasEdges() {
      return fieldSetFlags()[2];
    }

    /**
     * Clears the value of the 'edges' field.
     * @return org.apache.giraph.io.gora.generated.GVertexResult.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GVertexResult.Builder
    clearEdges() {
      edges = null;
      fieldSetFlags()[2] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    @Override
    /**
     * Builds a GVertexResult.
     * @return GVertexResult
     */
    // CHECKSTYLE: stop IllegalCatch
    public GVertexResult build() {
      try {
        GVertexResult record = new GVertexResult();
        record.vertexId = fieldSetFlags()[0] ? this.vertexId :
          (java.lang.CharSequence) defaultValue(fields()[0]);
        record.vertexValue = fieldSetFlags()[1] ? this.vertexValue :
          (java.lang.Float) defaultValue(fields()[1]);
        record.edges = fieldSetFlags()[2] ? this.edges :
          (java.util.Map<java.lang.CharSequence, java.lang.CharSequence>)
          new org.apache.gora.persistency.impl.DirtyMapWrapper(
            (java.util.Map) defaultValue(fields()[2]));
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
    // CHECKSTYLE: resume IllegalCatch
  }

  /**
   * Gets tombstone
   * @return GVertex.Tombstone
   */
  public GVertexResult.Tombstone getTombstone() {
    return TOMBSTONE;
  }

  /**
   * Gets a new instance
   * @return GVertexResult.
   */
  public GVertexResult newInstance() {
    return newBuilder().build();
  }

  /**
   * Tombstone class.
   */
  public static final class Tombstone extends GVertexResult implements
      org.apache.gora.persistency.Tombstone {

    /**
     * Default constructor.
     */
    private Tombstone() {
    }

    /**
     * Gets the value of the 'vertexId' field.
     * @return java.lang.CharSequence
     */
    public java.lang.CharSequence getVertexId() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'vertexId' field.
     * @param value the value to set.
     */
    public void setVertexId(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'vertexId' field. A field is dirty if it
     * represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isVertexIdDirty(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }

    /**
     * Gets the value of the 'vertexValue' field.
     * @return Float
     */
    public java.lang.Float getVertexValue() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'vertexValue' field.
     * @param value the value to set.
     */
    public void setVertexValue(java.lang.Float value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'vertexValue' field. A field is dirty if
     * it represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isVertexValueDirty(java.lang.Float value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }

    /**
     * Gets the value of the 'edges' field.
     * @return java.util.Map
     */
    public java.util.Map<java.lang.CharSequence, java.lang.CharSequence>
    getEdges() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'edges' field.
     * @param value the value to set.
     */
    public void setEdges(
        java.util.Map<java.lang.CharSequence, java.lang.CharSequence> value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'edges' field. A field is dirty if it
     * represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isEdgesDirty(
        java.util.Map<java.lang.CharSequence, java.lang.CharSequence> value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }
  }
}
