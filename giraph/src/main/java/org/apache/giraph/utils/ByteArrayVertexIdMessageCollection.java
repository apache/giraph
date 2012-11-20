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

package org.apache.giraph.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Vertex id message collection that stores everything in a single byte array
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class ByteArrayVertexIdMessageCollection<I extends WritableComparable,
    M extends Writable> implements Writable,
    ImmutableClassesGiraphConfigurable {
  /** Extended data output */
  private ExtendedDataOutput extendedDataOutput;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?, M> configuration;

  /**
   * Constructor for reflection
   */
  public ByteArrayVertexIdMessageCollection() { }

  /**
   * Initialize the inner state. Must be called before {@code add()} is
   * called.
   */
  public void initialize() {
    extendedDataOutput = configuration.createExtendedDataOutput();
  }

  /**
   * Initialize the inner state, with a known size. Must be called before
   * {@code add()} is called.
   *
   * @param expectedSize Number of bytes to be expected
   */
  public void initialize(int expectedSize) {
    extendedDataOutput = configuration.createExtendedDataOutput(expectedSize);
  }

  /**
   * Add a vertex id and message pair to the collection.
   *
   * @param vertexId Vertex id
   * @param message Message
   */
  public void add(I vertexId, M message) {
    try {
      vertexId.write(extendedDataOutput);
      message.write(extendedDataOutput);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  /**
   * Get the number of bytes used
   *
   * @return Number of bytes used
   */
  public int getSize() {
    return extendedDataOutput.getPos();
  }

  /**
   * Check if the list is empty.
   *
   * @return True iff there are no pairs in the list
   */
  public boolean isEmpty() {
    return extendedDataOutput.getPos() == 0;
  }

  /**
   * Get iterator through elements of this object.
   *
   * @return {@link Iterator} iterator
   */
  public Iterator getIterator() {
    return new Iterator();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return configuration;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(extendedDataOutput.getPos());
    dataOutput.write(extendedDataOutput.getByteArray(), 0,
        extendedDataOutput.getPos());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int size = dataInput.readInt();
    byte[] buf = new byte[size];
    dataInput.readFully(buf);
    extendedDataOutput = configuration.createExtendedDataOutput(buf, size);
  }

  /**
   * Special iterator class which we'll use to iterate through elements of
   * {@link PairList}, without having to create new object as wrapper for
   * each pair.
   *
   * Protocol is somewhat similar to the protocol of {@link java.util.Iterator}
   * only here next() doesn't return the next object, it just moves along in
   * the collection. Values related to current pair can be retrieved by calling
   * getCurrentFirst() and getCurrentSecond() methods.
   *
   * Not thread-safe.
   */
  public class Iterator {
    /** Reader of the serialized messages */
    private ExtendedDataInput extendedDataInput =
        configuration.createExtendedDataInput(
            extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
    /** Current vertex id */
    private I vertexId;
    /** Current message */
    private M message;

    /**
     * Returns true if the iteration has more elements.
     *
     * @return True if the iteration has more elements.
     */
    public boolean hasNext() {
      return extendedDataInput.available() > 0;
    }

    /**
     * Moves to the next element in the iteration.
     */
    public void next() {
      vertexId = configuration.createVertexId();
      message = configuration.createMessageValue();
      try {
        vertexId.readFields(extendedDataInput);
        message.readFields(extendedDataInput);
      } catch (IOException e) {
        throw new IllegalStateException("next: IOException", e);
      }
    }

    /**
     * Get the current vertex id
     *
     * @return Current vertex id
     */
    public I getCurrentVertexId() {
      return vertexId;
    }

    /**
     * Get the current message
     *
     * @return Current message
     */
    public M getCurrentMessage() {
      return message;
    }
  }
}
