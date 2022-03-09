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

import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Special iterator that reuses vertex ids and data objects so that the
 * lifetime of the object is only until next() is called.
 *
 * Vertex id ownership can be released if desired through
 * releaseCurrentVertexId().  This optimization allows us to cut down
 * on the number of objects instantiated and garbage collected.
 *
 * @param <I> vertexId type parameter
 * @param <T> vertexData type parameter
 */
@NotThreadSafe
public class ByteStructVertexIdDataIterator<I extends WritableComparable, T>
  extends ByteStructVertexIdIterator<I> implements VertexIdDataIterator<I, T> {
  /** VertexIdData to iterate over */
  protected AbstractVertexIdData<I, T> vertexIdData;
  /** Serialized size of the message object in bytestore */
  protected int dataSize;
  /** Current data. */
  private T data;

  /**
   * Constructor
   *
   * @param vertexIdData vertexIdData
   */
  public ByteStructVertexIdDataIterator(
    AbstractVertexIdData<I, T> vertexIdData) {
    super(vertexIdData.extendedDataOutput, vertexIdData.getConf());
    this.vertexIdData = vertexIdData;
  }

  @Override
  public void next() {
    if (vertexId == null) {
      vertexId = vertexIdData.getConf().createVertexId();
    }
    if (data == null) {
      data = vertexIdData.createData();
    }
    try {
      vertexId.readFields(extendedDataInput);
      int initial = extendedDataInput.getPos();
      vertexIdData.readData(extendedDataInput, data);
      dataSize = extendedDataInput.getPos() - initial;
    } catch (IOException e) {
      throw new IllegalStateException("next: IOException", e);
    }
  }

  @Override
  public int getCurrentDataSize() {
    return dataSize;
  }

  @Override
  public T getCurrentData() {
    return data;
  }

  @Override
  public T releaseCurrentData() {
    T releasedData = data;
    data = null;
    return releasedData;
  }
}
