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

import java.io.IOException;

/**
 * Stores a message and a list of target vertex ids.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class ByteArrayOneMessageToManyIds<I extends WritableComparable,
  M extends Writable> extends ByteArrayVertexIdData<I, M>
  implements VertexIdMessages<I, M> {
  /** Message value class */
  private MessageValueFactory<M> messageValueFactory;

  /**
   * Constructor.
   *
   * @param messageValueFactory Class for messages
   */
  public ByteArrayOneMessageToManyIds(
      MessageValueFactory<M> messageValueFactory) {
    this.messageValueFactory = messageValueFactory;
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

  /**
   * Add a message.
   * The order is: the message &gt; id count &gt; ids .
   *
   * @param ids   The byte array which holds target ids
   *              of this message on the worker
   * @param idPos The end position of the ids
   *              information in the byte array above.
   * @param count The number of ids
   * @param msg   The message sent
   */
  public void add(byte[] ids, int idPos, int count, M msg) {
    try {
      msg.write(extendedDataOutput);
      extendedDataOutput.writeInt(count);
      extendedDataOutput.write(ids, 0, idPos);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  @Override
  public void add(I vertexId, M data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(byte[] serializedId, int idPos, M data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VertexIdMessageBytesIterator<I, M> getVertexIdMessageBytesIterator() {
    return null;
  }

  @Override
  public VertexIdMessageIterator<I, M> getVertexIdMessageIterator() {
    return new OneMessageToManyIdsIterator<>(this);
  }
}
