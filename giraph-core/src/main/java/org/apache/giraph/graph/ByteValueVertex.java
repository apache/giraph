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
package org.apache.giraph.graph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.IOException;


/**
 * Special version of vertex that holds the value in raw byte form to save
 * memory.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class ByteValueVertex<I extends WritableComparable,
        V extends Writable, E extends Writable>
        extends DefaultVertex<I, V, E> {

  /** Vertex value stored as raw bytes */
  private byte[] valueBytes;
  /** Value as an cached object that is only valid during the vertex update */
  private V cachedValue = null;

  @Override
  public V getValue() {
    if (cachedValue != null) {
      return cachedValue; // Return always same instance
    }
    DataInput dis = new UnsafeByteArrayInputStream(valueBytes);
    cachedValue = getConf().createVertexValue();
    try {
      cachedValue.readFields(dis);
    } catch (IOException ioe) {
      throw new RuntimeException("Could not deserialize vertex value", ioe);
    }
    // Forget the serialized data, because we have cached the object
    valueBytes = null;
    return cachedValue;
  }

  /**
   * Serializes the value to bytes, stored in field valueBytes
   * @param value new vertex value
   */
  private void setSerializedValue(V value) {
    UnsafeByteArrayOutputStream bos = new UnsafeByteArrayOutputStream();
    try {
      value.write(bos);
      bos.close();
    } catch (IOException ioe) {
      throw new RuntimeException("Could not serialize vertex value", ioe);
    }
    this.valueBytes = bos.toByteArray();
    cachedValue = null;
  }

  @Override
  public void setValue(V value) {
    if (cachedValue != null) {
      cachedValue = value;
    } else {
      setSerializedValue(value);
    }
  }

  @Override
  public void initialize(I id, V value, Iterable<Edge<I, E>> edges) {
    // Set the parent's value to null, and instead use our own setter
    super.initialize(id, null, edges);
    setValue(value);
  }

  @Override
  public void initialize(I id, V value) {
    super.initialize(id, null);
    setValue(value);
  }

  @Override
  public void unwrapMutableEdges() {
    // This method is called always after compute(vertex), so
    // we use this to commit the vertex value.
    if (cachedValue != null) {
      // This means the value has been requested from vertex
      // and possible mutated -- so we need to update the byte array
      setSerializedValue(cachedValue);
      cachedValue = null;  // Uncache the value
    }
    super.unwrapMutableEdges();
  }
}
