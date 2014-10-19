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
 * Example class for defining a Giraph-Edge.
 */
@SuppressWarnings("all")
public class GEdge extends org.apache.gora.persistency.impl.PersistentBase
    implements org.apache.avro.specific.SpecificRecord,
    org.apache.gora.persistency.Persistent {

  /**
   * Schema used for the class.
   */
  public static final org.apache.avro.Schema SCHEMAS =
      new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\"," +
          "\"name\":\"GEdge\"," +
          "\"namespace\":\"org.apache.giraph.gora.generated\"," +
          "\"fields\":[{\"name\":\"edgeId\",\"type\":\"string\"}," +
          "{\"name\":\"edgeWeight\",\"type\":\"float\"}," +
          "{\"name\":\"vertexInId\",\"type\":\"string\"}," +
          "{\"name\":\"vertexOutId\",\"type\":\"string\"}," +
          "{\"name\":\"label\",\"type\":\"string\"}]}");

  /** Enum containing all data bean's fields. */
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
    "edgeId", "edgeWeight", "vertexInId", "vertexOutId", "label"};

  /**
   * Tombstone.
   */
  private static final Tombstone TOMBSTONE = new Tombstone();

  /**
   * edgeId.
   */
  private java.lang.CharSequence edgeId;

  /**
   * edgeWeight.
   */
  private float edgeWeight;

  /**
   * vertexInId.
   */
  private java.lang.CharSequence vertexInId;

  /**
   * vertexOutId.
   */
  private java.lang.CharSequence vertexOutId;

  /**
   * label.
   */
  private java.lang.CharSequence label;

  /**
   * Gets the total field count.
   * @return int field count
   */
  public int getFieldsCount() {
    return GEdge.ALL_FIELDS.length;
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
      edgeId = (java.lang.CharSequence) value;
      break;
    case 1:
      edgeWeight = (java.lang.Float) value;
      break;
    case 2:
      vertexInId = (java.lang.CharSequence) value;
      break;
    case 3:
      vertexOutId = (java.lang.CharSequence) value;
      break;
    case 4:
      label = (java.lang.CharSequence) value;
      break;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'edgeId' field.
   * @return CharSequence.
   */
  public java.lang.CharSequence getEdgeId() {
    return edgeId;
  }

  /**
   * Sets the value of the 'edgeId' field.
   * @param value the value to set.
   */
  public void setEdgeId(java.lang.CharSequence value) {
    this.edgeId = value;
    setDirty(0);
  }

  /**
   * Checks the dirty status of the 'edgeId' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isEdgeIdDirty(java.lang.CharSequence value) {
    return isDirty(0);
  }

  /**
   * Gets the value of the 'edgeWeight' field.
   * @return Float
   */
  public java.lang.Float getEdgeWeight() {
    return edgeWeight;
  }

  /**
   * Sets the value of the 'edgeWeight' field.
   * @param value the value to set.
   */
  public void setEdgeWeight(java.lang.Float value) {
    this.edgeWeight = value;
    setDirty(1);
  }

  /**
   * Checks the dirty status of the 'edgeWeight' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isEdgeWeightDirty(java.lang.Float value) {
    return isDirty(1);
  }

  /**
   * Gets the value of the 'vertexInId' field.
   * @return CharSequence
   */
  public java.lang.CharSequence getVertexInId() {
    return vertexInId;
  }

  /**
   * Sets the value of the 'vertexInId' field.
   * @param value the value to set.
   */
  public void setVertexInId(java.lang.CharSequence value) {
    this.vertexInId = value;
    setDirty(2);
  }

  /**
   * Checks the dirty status of the 'vertexInId' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isVertexInIdDirty(java.lang.CharSequence value) {
    return isDirty(2);
  }

  /**
   * Gets the value of the 'vertexOutId' field.
   * @return CharSequence
   */
  public java.lang.CharSequence getVertexOutId() {
    return vertexOutId;
  }

  /**
   * Sets the value of the 'vertexOutId' field.
   * @param value the value to set.
   */
  public void setVertexOutId(java.lang.CharSequence value) {
    this.vertexOutId = value;
    setDirty(3);
  }

  /**
   * Checks the dirty status of the 'vertexOutId' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isVertexOutIdDirty(java.lang.CharSequence value) {
    return isDirty(3);
  }

  /**
   * Gets the value of the 'label' field.
   * @return CharSequence
   */
  public java.lang.CharSequence getLabel() {
    return label;
  }

  /**
   * Sets the value of the 'label' field.
   * @param value the value to set.
   */
  public void setLabel(java.lang.CharSequence value) {
    this.label = value;
    setDirty(4);
  }

  /**
   * Checks the dirty status of the 'label' field. A field is dirty if it
   * represents a change that has not yet been written to the database.
   * @param value the value to set.
   * @return boolean
   */
  public boolean isLabelDirty(java.lang.CharSequence value) {
    return isDirty(4);
  }

  /**
   * Creates a new GEdge RecordBuilder.
   * @return GEdge.Builder
   */
  public static org.apache.giraph.io.gora.generated.GEdge.Builder newBuilder() {
    return new org.apache.giraph.io.gora.generated.GEdge.Builder();
  }

  /**
   * Creates a new GEdge RecordBuilder by copying an existing Builder
   * @param other GEdge.Builder
   * @return org.apache.giraph.io.gora.generated.GEdge.Builder
   */
  public static org.apache.giraph.io.gora.generated.GEdge.Builder newBuilder(
      org.apache.giraph.io.gora.generated.GEdge.Builder other) {
    return new org.apache.giraph.io.gora.generated.GEdge.Builder(other);
  }

  /**
   * Creates a new GEdge RecordBuilder by copying an existing GEdge instance.
   * @param other GEdge
   * @return org.apache.giraph.io.gora.generated.GEdge.Builder
   */
  public static org.apache.giraph.io.gora.generated.GEdge.Builder newBuilder(
      org.apache.giraph.io.gora.generated.GEdge other) {
    return new org.apache.giraph.io.gora.generated.GEdge.Builder(other);
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
   * RecordBuilder for GEdge instances.
   */
  public static class Builder extends
      org.apache.avro.specific.SpecificRecordBuilderBase<GEdge> implements
      org.apache.avro.data.RecordBuilder<GEdge> {

    /**
     * edgeId.
     */
    private java.lang.CharSequence edgeId;

    /**
     * edgeWeight.
     */
    private float edgeWeight;

    /**
     * vertexInId
     */
    private java.lang.CharSequence vertexInId;

    /**
     * vertexOutId.
     */
    private java.lang.CharSequence vertexOutId;

    /**
     * label.
     */
    private java.lang.CharSequence label;

    /**
     * Creates a new Builder
     */
    private Builder() {
      super(org.apache.giraph.io.gora.generated.GEdge.SCHEMAS);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other GEdge.Builder
     */
    private Builder(org.apache.giraph.io.gora.generated.GEdge.Builder other) {
      super(other);
    }

    /**
     * Creates a Builder by copying an existing GEdge instance.
     * @param other GEdge
     */
    // CHECKSTYLE: stop Indentation
    private Builder(org.apache.giraph.io.gora.generated.GEdge other) {
      super(org.apache.giraph.io.gora.generated.GEdge.SCHEMAS);
      if (isValidValue(fields()[0], other.edgeId)) {
        this.edgeId = (java.lang.CharSequence) data().deepCopy(
          fields()[0].schema(), other.edgeId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.edgeWeight)) {
        this.edgeWeight = (java.lang.Float) data().deepCopy(
          fields()[1].schema(), other.edgeWeight);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.vertexInId)) {
        this.vertexInId = (java.lang.CharSequence) data().deepCopy(
          fields()[2].schema(), other.vertexInId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.vertexOutId)) {
        this.vertexOutId = (java.lang.CharSequence) data().deepCopy(
          fields()[3].schema(), other.vertexOutId);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.label)) {
        this.label = (java.lang.CharSequence) data().deepCopy(
          fields()[4].schema(), other.label);
        fieldSetFlags()[4] = true;
      }
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'edgeId' field
     * @return CharSequence
     */
    public java.lang.CharSequence getEdgeId() {
      return edgeId;
    }

    /**
     * Sets the value of the 'edgeId' field
     * @param value CharSequence
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder setEdgeId(
      java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.edgeId = value;
      fieldSetFlags()[0] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'edgeId' field has been set.
     * @return boolean.
     */
    public boolean hasEdgeId() {
      return fieldSetFlags()[0];
    }

    /**
     * Clears the value of the 'edgeId' field.
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder clearEdgeId() {
      edgeId = null;
      fieldSetFlags()[0] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'edgeWeight' field.
     * @return Float
     */
    public java.lang.Float getEdgeWeight() {
      return edgeWeight;
    }

    /**
     * Sets the value of the 'edgeWeight' field
     * @param value float
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder setEdgeWeight(
      float value) {
      validate(fields()[1], value);
      this.edgeWeight = value;
      fieldSetFlags()[1] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'edgeWeight' field has been set.
     * @return boolean
     */
    public boolean hasEdgeWeight() {
      return fieldSetFlags()[1];
    }

    /**
     * Clears the value of the 'edgeWeight' field.
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder clearEdgeWeight() {
      fieldSetFlags()[1] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'vertexInId' field
     * @return CharSequence
     */
    public java.lang.CharSequence getVertexInId() {
      return vertexInId;
    }

    /**
     * Sets the value of the 'vertexInId' field.
     * @param value CharSequence
     * @return value
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder setVertexInId(
      java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.vertexInId = value;
      fieldSetFlags()[2] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'vertexInId' field has been set.
     * @return boolean
     */
    public boolean hasVertexInId() {
      return fieldSetFlags()[2];
    }

    /**
     * Clears the value of the 'vertexInId' field.
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder
    clearVertexInId() {
      vertexInId = null;
      fieldSetFlags()[2] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'vertexOutId' field.
     * @return java.lang.CharSequence
     */
    public java.lang.CharSequence getVertexOutId() {
      return vertexOutId;
    }

    /**
     * Sets the value of the 'vertexOutId' field.
     * @param value CharSequence
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder setVertexOutId(
        java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.vertexOutId = value;
        fieldSetFlags()[3] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'vertexOutId' field has been set.
     * @return boolean
     */
    public boolean hasVertexOutId() {
      return fieldSetFlags()[3];
    }

    /**
     * Clears the value of the 'vertexOutId' field.
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder
    clearVertexOutId() {
      vertexOutId = null;
      fieldSetFlags()[3] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Gets the value of the 'label' field.
     * @return CharSequence
     */
    public java.lang.CharSequence getLabel() {
      return label;
    }

    /**
     * Sets the value of the 'label' field.
     * @param value CharSequence
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder setLabel(
        java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.label = value;
      fieldSetFlags()[4] = true;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    /**
     * Checks whether the 'label' field has been set.
     * @return boolean
     */
    public boolean hasLabel() {
      return fieldSetFlags()[4];
    }

    /**
     * Clears the value of the 'label' field.
     * @return GEdge.Builder
     */
    // CHECKSTYLE: stop Indentation
    public org.apache.giraph.io.gora.generated.GEdge.Builder clearLabel() {
      label = null;
      fieldSetFlags()[4] = false;
      return this;
    }
    // CHECKSTYLE: resume Indentation

    @Override
    /**
     * Builds a GEdge.
     * @return GEdge
     */
    // CHECKSTYLE: stop IllegalCatch
    public GEdge build() {
      try {
        GEdge record = new GEdge();
        record.edgeId = fieldSetFlags()[0] ? this.edgeId :
          (java.lang.CharSequence) defaultValue(fields()[0]);
        record.edgeWeight = fieldSetFlags()[1] ? this.edgeWeight :
          (java.lang.Float) defaultValue(fields()[1]);
        record.vertexInId = fieldSetFlags()[2] ? this.vertexInId :
          (java.lang.CharSequence) defaultValue(fields()[2]);
        record.vertexOutId = fieldSetFlags()[3] ? this.vertexOutId :
          (java.lang.CharSequence) defaultValue(fields()[3]);
        record.label = fieldSetFlags()[4] ? this.label :
          (java.lang.CharSequence) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
    // CHECKSTYLE: resume IllegalCatch
  }

  /**
   * Gets a tombstone
   * @return GEdge.Tombstone
   */
  public GEdge.Tombstone getTombstone() {
    return TOMBSTONE;
  }

  /**
   * Gets a new instance
   * @return GEdge.
   */
  public GEdge newInstance() {
    return newBuilder().build();
  }

  /**
   * Tombstone class.
   */
  public static final class Tombstone extends GEdge implements
      org.apache.gora.persistency.Tombstone {

    /**
     * Default constructor.
     */
    private Tombstone() {
    }

    /**
     * Gets the value of the 'edgeId' field.
     * @return java.lang.CharSequence
     */
    public java.lang.CharSequence getEdgeId() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'edgeId' field.
     * @param value the value to set.
     */
    public void setEdgeId(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'edgeId' field. A field is dirty if it
     * represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isEdgeIdDirty(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }

    /**
     * Gets the value of the 'edgeWeight' field.
     * @return Float
     */
    public java.lang.Float getEdgeWeight() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'edgeWeight' field.
     * @param value the value to set.
     */
    public void setEdgeWeight(java.lang.Float value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'edgeWeight' field. A field is dirty if it
     * represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isEdgeWeightDirty(java.lang.Float value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }

    /**
     * Gets the value of the 'vertexInId' field.
     * @return CharSequence
     */
    public java.lang.CharSequence getVertexInId() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'vertexInId' field.
     * @param value the value to set.
     */
    public void setVertexInId(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'vertexInId' field. A field is dirty if it
     * represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isVertexInIdDirty(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }

    /**
     * Gets the value of the 'vertexOutId' field.
     * @return CharSequence
     */
    public java.lang.CharSequence getVertexOutId() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'vertexOutId' field.
     * @param value the value to set.
     */
    public void setVertexOutId(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'vertexOutId' field. A field is dirty if
     * it represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isVertexOutIdDirty(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }

    /**
     * Gets the value of the 'label' field.
     * @return CharSequence
     */
    public java.lang.CharSequence getLabel() {
      throw new java.lang.UnsupportedOperationException(
          "Get is not supported on tombstones");
    }

    /**
     * Sets the value of the 'label' field.
     * @param value the value to set.
     */
    public void setLabel(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "Set is not supported on tombstones");
    }

    /**
     * Checks the dirty status of the 'label' field. A field is dirty if it
     * represents a change that has not yet been written to the database.
     * @param value the value to set.
     * @return boolean
     */
    public boolean isLabelDirty(java.lang.CharSequence value) {
      throw new java.lang.UnsupportedOperationException(
          "IsDirty is not supported on tombstones");
    }
  }
}
