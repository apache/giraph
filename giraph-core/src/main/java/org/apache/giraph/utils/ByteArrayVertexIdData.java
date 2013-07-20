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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores pairs of vertex id and generic data in a single byte array
 *
 * @param <I> Vertex id
 * @param <T> Data
 */
public abstract class ByteArrayVertexIdData<I extends WritableComparable, T>
  implements Writable, ImmutableClassesGiraphConfigurable {
  /** Extended data output */
  private ExtendedDataOutput extendedDataOutput;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?> configuration;

  /**
   * Create a new data object.
   *
   * @return Newly-created data object.
   */
  public abstract T createData();

  /**
   * Write a data object to an {@link ExtendedDataOutput}.
   *
   * @param out {@link ExtendedDataOutput}
   * @param data Data object to write
   * @throws IOException
   */
  public abstract void writeData(ExtendedDataOutput out, T data)
    throws IOException;

  /**
   * Read a data object's fields from an {@link ExtendedDataInput}.
   *
   * @param in {@link ExtendedDataInput}
   * @param data Data object to fill in-place
   * @throws IOException
   */
  public abstract void readData(ExtendedDataInput in, T data)
    throws IOException;

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
   * Add a vertex id and data pair to the collection.
   *
   * @param vertexId Vertex id
   * @param data Data
   */
  public void add(I vertexId, T data) {
    try {
      vertexId.write(extendedDataOutput);
      writeData(extendedDataOutput, data);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  /**
   * Add a serialized vertex id and data.
   *
   * @param serializedId The bye array which holds the serialized id.
   * @param idPos The end position of the serialized id in the byte array.
   * @param data Data
   */
  public void add(byte[] serializedId, int idPos, T data) {
    try {
      extendedDataOutput.write(serializedId, 0, idPos);
      writeData(extendedDataOutput, data);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  /**
   * Get the number of bytes used.
   *
   * @return Bytes used
   */
  public int getSize() {
    return extendedDataOutput.getPos();
  }

  /**
   * Get the size of this object in serialized form.
   *
   * @return The size (in bytes) of the serialized object
   */
  public int getSerializedSize() {
    return 1 + 4 + getSize();
  }

  /**
   * Check if the list is empty.
   *
   * @return Whether the list is empty
   */
  public boolean isEmpty() {
    return extendedDataOutput.getPos() == 0;
  }

  /**
   * Clear the list.
   */
  public void clear() {
    extendedDataOutput.reset();
  }

  /**
   * Get the underlying byte-array.
   *
   * @return The underlying byte-array
   */
  public byte[] getByteArray() {
    return extendedDataOutput.getByteArray();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, ?, ?> getConf() {
    return configuration;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeExtendedDataOutput(extendedDataOutput, dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    extendedDataOutput =
        WritableUtils.readExtendedDataOutput(dataInput, configuration);
  }

  /**
   * Get an iterator over the pairs.
   *
   * @return Iterator
   */
  public VertexIdDataIterator getVertexIdDataIterator() {
    return new VertexIdDataIterator();
  }

  /**
   * Special iterator that reuses vertex ids and data objects so that the
   * lifetime of the object is only until next() is called.
   *
   * Vertex id ownership can be released if desired through
   * releaseCurrentVertexId().  This optimization allows us to cut down
   * on the number of objects instantiated and garbage collected.
   *
   * Not thread-safe.
   */
  public class VertexIdDataIterator extends VertexIdIterator<I> {
    /** Current data. */
    private T data;

    /** Default constructor. */
    public VertexIdDataIterator() {
      super(extendedDataOutput, configuration);
    }

    @Override
    public void next() {
      if (vertexId == null) {
        vertexId = configuration.createVertexId();
      }
      if (data == null) {
        data = createData();
      }
      try {
        vertexId.readFields(extendedDataInput);
        readData(extendedDataInput, data);
      } catch (IOException e) {
        throw new IllegalStateException("next: IOException", e);
      }
    }

    /**
     * Get the current data.
     *
     * @return Current data
     */
    public T getCurrentData() {
      return data;
    }

    /**
     * Release the current data object.
     *
     * @return Released data object
     */
    public T releaseCurrentData() {
      T releasedData = data;
      data = null;
      return releasedData;
    }
  }

}

