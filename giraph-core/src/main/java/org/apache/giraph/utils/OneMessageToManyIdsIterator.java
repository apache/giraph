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

/**
 * VertexIdData iterator for
 * {@link ByteArrayOneMessageToManyIds}
 *
 * @param <I> vertexId type
 * @param <M> message type
 */
public class OneMessageToManyIdsIterator<I extends WritableComparable,
    M extends Writable> implements VertexIdMessageIterator<I, M> {
  /** VertexIdMessages object to iterate over */
  private final ByteArrayOneMessageToManyIds<I, M> vertexIdMessages;
  /** Reader of the serialized edges */
  private final ExtendedDataInput extendedDataInput;

  /** Current vertex Id*/
  private I vertexId;
  /** Current message */
  private M msg;
  /** Counts of ids left to read before next message */
  private int idsToRead = 0;
  /** Size of message read */
  private int msgSize = 0;
  /** Is current message newly read */
  private boolean newMessage;

  /**
   * Constructor
   *
   * @param vertexIdMessages vertexId messages object to iterate over
   */
  public OneMessageToManyIdsIterator(
      final ByteArrayOneMessageToManyIds<I, M> vertexIdMessages) {
    this.vertexIdMessages = vertexIdMessages;
    this.extendedDataInput = vertexIdMessages.getConf()
        .createExtendedDataInput(vertexIdMessages.extendedDataOutput);
  }

  @Override
  public I getCurrentVertexId() {
    return vertexId;
  }

  @Override
  public M getCurrentMessage() {
    return getCurrentData();
  }

  @Override
  public M getCurrentData() {
    return msg;
  }

  @Override
  public M releaseCurrentData() {
    M releasedData = msg;
    msg = null;
    return releasedData;
  }

  @Override
  public I releaseCurrentVertexId() {
    I releasedVertexId = vertexId;
    vertexId = null;
    return releasedVertexId;
  }

  @Override
  public boolean hasNext() {
    return extendedDataInput.available() > 0;
  }

  /**
   * Properly initialize vertexId & msg object before calling next()
   */
  private void initialize() {
    if (vertexId == null) {
      vertexId = vertexIdMessages.getConf().createVertexId();
    }
    if (msg == null) {
      msg = vertexIdMessages.createData();
    }
  }

  @Override
  public void next() {
    initialize();
    try {
      if (idsToRead == 0) {
        newMessage = true; // a new message is read
        int initial = extendedDataInput.getPos();
        msg.readFields(extendedDataInput);
        msgSize = extendedDataInput.getPos() - initial;
        idsToRead = extendedDataInput.readInt();
      } else {
        newMessage = false; // same as previous message
      }
      vertexId.readFields(extendedDataInput);
      idsToRead -= 1;
    } catch (IOException e) {
      throw new IllegalStateException("next: IOException", e);
    }
  }

  @Override
  public int getCurrentMessageSize() {
    return getCurrentDataSize();
  }

  @Override
  public int getCurrentDataSize() {
    return msgSize;
  }

  @Override
  public boolean isNewMessage() {
    return newMessage;
  }
}
