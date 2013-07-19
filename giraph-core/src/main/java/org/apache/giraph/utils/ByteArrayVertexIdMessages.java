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

import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores vertex id and message pairs in a single byte array.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class ByteArrayVertexIdMessages<I extends WritableComparable,
    M extends Writable> extends ByteArrayVertexIdData<I, M> {
  /** Message value class */
  private MessageValueFactory<M> messageValueFactory;
  /** Add the message size to the stream? (Depends on the message store) */
  private boolean useMessageSizeEncoding = false;

  /**
   * Constructor
   *
   * @param messageValueFactory Class for messages
   */
  public ByteArrayVertexIdMessages(
      MessageValueFactory<M> messageValueFactory) {
    this.messageValueFactory = messageValueFactory;
  }

  /**
   * Set whether message sizes should be encoded.  This should only be a
   * possibility when not combining.  When combining, all messages need to be
   * deserializd right away, so this won't help.
   */
  private void setUseMessageSizeEncoding() {
    if (!getConf().useMessageCombiner()) {
      useMessageSizeEncoding = getConf().useMessageSizeEncoding();
    } else {
      useMessageSizeEncoding = false;
    }
  }

  @Override
  public M createData() {
    return messageValueFactory.newInstance();
  }

  @Override
  public void writeData(ExtendedDataOutput out, M message) throws IOException {
    message.write(out);
  }

  @Override
  public void readData(ExtendedDataInput in, M message) throws IOException {
    message.readFields(in);
  }

  @Override
  public void initialize() {
    super.initialize();
    setUseMessageSizeEncoding();
  }

  @Override
  public void initialize(int expectedSize) {
    super.initialize(expectedSize);
    setUseMessageSizeEncoding();
  }

  /**
   * Get specialized iterator that will instiantiate the vertex id and
   * message of this object.
   *
   * @return Special iterator that reuses vertex ids and messages unless
   *         specified
   */
  public VertexIdMessageIterator getVertexIdMessageIterator() {
    return new VertexIdMessageIterator();
  }

  /**
   * Special iterator that reuses vertex ids and message objects so that the
   * lifetime of the object is only until next() is called.
   */
  public class VertexIdMessageIterator extends VertexIdDataIterator {
    /**
     * Get the current message.
     *
     * @return Current message
     */
    public M getCurrentMessage() {
      return getCurrentData();
    }
  }

  /**
   * Get specialized iterator that will instiantiate the vertex id and
   * message of this object.  It will only produce message bytes, not actual
   * messages and expects a different encoding.
   *
   * @return Special iterator that reuses vertex ids (unless released) and
   *         copies message bytes
   */
  public VertexIdMessageBytesIterator getVertexIdMessageBytesIterator() {
    if (!useMessageSizeEncoding) {
      return null;
    }
    return new VertexIdMessageBytesIterator();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeBoolean(useMessageSizeEncoding);
    super.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    useMessageSizeEncoding = dataInput.readBoolean();
    super.readFields(dataInput);
  }

  /**
   * Special iterator that reuses vertex ids and messages bytes so that the
   * lifetime of the object is only until next() is called.
   *
   * Vertex id ownership can be released if desired through
   * releaseCurrentVertexId().  This optimization allows us to cut down
   * on the number of objects instantiated and garbage collected.  Messages
   * can only be copied to an ExtendedDataOutput object
   *
   * Not thread-safe.
   */
  public class VertexIdMessageBytesIterator extends VertexIdDataIterator {
    /** Last message offset */
    private int messageOffset = -1;
    /** Number of bytes in the last message */
    private int messageBytes = -1;

    /**
     * Moves to the next element in the iteration.
     */
    @Override
    public void next() {
      if (vertexId == null) {
        vertexId = getConf().createVertexId();
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

    /**
     * Write the current message to an ExtendedDataOutput object
     *
     * @param dataOutput Where the current message will be written to
     */
    public void writeCurrentMessageBytes(DataOutput dataOutput) {
      try {
        dataOutput.write(getByteArray(), messageOffset, messageBytes);
      } catch (IOException e) {
        throw new IllegalStateException("writeCurrentMessageBytes: Got " +
            "IOException", e);
      }
    }
  }
}
