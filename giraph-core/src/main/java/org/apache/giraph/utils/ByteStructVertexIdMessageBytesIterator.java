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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Special iterator that reuses vertex ids and messages bytes so that the
 * lifetime of the object is only until next() is called.
 *
 * Vertex id ownership can be released if desired through
 * releaseCurrentVertexId().  This optimization allows us to cut down
 * on the number of objects instantiated and garbage collected.  Messages
 * can only be copied to an ExtendedDataOutput object
 *
 * @param <I> vertexId type parameter
 * @param <M> message type parameter
 */
@NotThreadSafe
public abstract class ByteStructVertexIdMessageBytesIterator<I
  extends WritableComparable, M extends Writable>
  extends ByteStructVertexIdDataIterator<I, M>
  implements VertexIdMessageBytesIterator<I, M> {
  /** Last message offset */
  protected int messageOffset = -1;
  /** Number of bytes in the last message */
  protected int messageBytes = -1;

  /**
   * Constructor with vertexIdData
   *
   * @param vertexIdData vertexIdData
   */
  public ByteStructVertexIdMessageBytesIterator(
    AbstractVertexIdData<I, M> vertexIdData) {
    super(vertexIdData);
  }

  /**
   * Moves to the next element in the iteration.
   */
  @Override
  public void next() {
    if (vertexId == null) {
      vertexId = vertexIdData.getConf().createVertexId();
    }

    try {
      vertexId.readFields(extendedDataInput);
      messageBytes = extendedDataInput.readInt();
      messageOffset = extendedDataInput.getPos();
      if (extendedDataInput.skipBytes(messageBytes) != messageBytes) {
        throw new IllegalStateException("next: Failed to skip " +
            messageBytes);
      }
    } catch (IOException e) {
      throw new IllegalStateException("next: IOException", e);
    }
  }
}
