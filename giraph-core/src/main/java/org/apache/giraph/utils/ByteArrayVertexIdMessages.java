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

import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Stores vertex id and message pairs in a single byte array.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class ByteArrayVertexIdMessages<I extends WritableComparable,
  M extends Writable> extends ByteArrayVertexIdData<I, M>
  implements VertexIdMessages<I, M> {
  /** Message value class */
  private final MessageValueFactory<M> messageValueFactory;
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
   * de-serialized right away, so this won't help.
   */
  private void setUseMessageSizeEncoding() {
    if (!getConf().useOutgoingMessageCombiner()) {
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

  @Override
  public ByteStructVertexIdMessageIterator<I, M> getVertexIdMessageIterator() {
    return new ByteStructVertexIdMessageIterator<>(this);
  }

  @Override
  public void add(I vertexId, M message) {
    if (!useMessageSizeEncoding) {
      super.add(vertexId, message);
    } else {
      try {
        vertexId.write(extendedDataOutput);
        writeMessageWithSize(message);
      } catch (IOException e) {
        throw new IllegalStateException("add: IOException occurred");
      }
    }
  }

  @Override
  public void add(byte[] serializedId, int idPos, M message) {
    if (!useMessageSizeEncoding) {
      super.add(serializedId, idPos, message);
    } else {
      try {
        extendedDataOutput.write(serializedId, 0, idPos);
        writeMessageWithSize(message);
      } catch (IOException e) {
        throw new IllegalStateException("add: IOException occurred");
      }
    }
  }

  /**
   * Write a size of the message and message
   *
   * @param message Message to write
   */
  private void writeMessageWithSize(M message) throws IOException {
    int pos = extendedDataOutput.getPos();
    extendedDataOutput.skipBytes(4);
    writeData(extendedDataOutput, message);
    extendedDataOutput.writeInt(
        pos, extendedDataOutput.getPos() - pos - 4);
  }

  @Override
  public ByteStructVertexIdMessageBytesIterator<I, M>
  getVertexIdMessageBytesIterator() {
    if (!useMessageSizeEncoding) {
      return null;
    }
    return new ByteStructVertexIdMessageBytesIterator<I, M>(this) {
      @Override
      public void writeCurrentMessageBytes(DataOutput dataOutput) {
        try {
          dataOutput.write(extendedDataOutput.getByteArray(),
            messageOffset, messageBytes);
        } catch (NegativeArraySizeException e) {
          VerboseByteStructMessageWrite.handleNegativeArraySize(vertexId);
        } catch (IOException e) {
          throw new IllegalStateException("writeCurrentMessageBytes: Got " +
              "IOException", e);
        }
      }
    };
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
}
