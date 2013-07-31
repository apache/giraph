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
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores a message and a list of target vertex ids.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class ByteArrayOneToAllMessages<
    I extends WritableComparable, M extends Writable>
    implements Writable, ImmutableClassesGiraphConfigurable {
  /** Extended data output */
  private ExtendedDataOutput extendedDataOutput;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?> configuration;
  /** Message value class */
  private MessageValueFactory<M> messageValueFactory;

  /**
   * Constructor.
   *
   * @param messageValueFactory Class for messages
   */
  public ByteArrayOneToAllMessages(
      MessageValueFactory<M> messageValueFactory) {
    this.messageValueFactory = messageValueFactory;
  }

  /**
   * Initialize the inner state. Must be called before {@code add()} is called.
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

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return this.configuration;
  }

  /**
   * Add a message.
   * The order is: the message>id count>ids .
   *
   * @param ids The byte array which holds target ids
   *                     of this message on the worker
   * @param idPos The end position of the ids
   *                     information in the byte array above.
   * @param count The number of ids
   * @param msg The message sent
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

  /**
   * Create a message.
   *
   * @return A created message object.
   */
  public M createMessage() {
    return messageValueFactory.newInstance();
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
   * Get the size of ByteArrayOneToAllMessages after serialization.
   * Here 4 is the size of an integer which represents the size of whole
   * byte array.
   *
   * @return The size (in bytes) of the serialized object
   */
  public int getSerializedSize() {
    return  4 + getSize();
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
   * Check if the byte array is empty.
   *
   * @return True if the position of the byte array is 0.
   */
  public boolean isEmpty() {
    return extendedDataOutput.getPos() == 0;
  }

  /**
   * Get the reader of this OneToAllMessages
   *
   * @return ExtendedDataInput
   */
  public ExtendedDataInput getOneToAllMessagesReader() {
    return configuration.createExtendedDataInput(
      extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
  }
}
