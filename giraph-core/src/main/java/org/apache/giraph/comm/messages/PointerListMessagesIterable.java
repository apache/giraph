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

package org.apache.giraph.comm.messages;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongListIterator;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedByteArrayOutputBuffer;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.hadoop.io.Writable;

/**
 * Create an iterable for messages based on a pointer list
 *
 * @param <M> messageType
 */
public class PointerListMessagesIterable<M extends Writable>
  implements Iterable<M> {
  /** Message class */
  private final MessageValueFactory<M> messageValueFactory;
  /** List of pointers to messages in byte array */
  private final LongArrayList pointers;
  /** Holds the byte arrays of serialized messages */
  private final ExtendedByteArrayOutputBuffer msgBuffer;
  /** Reader to read data from byte buffer */
  private final UnsafeReusableByteArrayInput messageReader;

  /**
   *
   * @param messageValueFactory message value factory
   * @param pointers pointers to messages in buffer
   * @param msgBuffer holds the byte arrays of serialized messages
   */
  public PointerListMessagesIterable(MessageValueFactory<M> messageValueFactory,
    LongArrayList pointers, ExtendedByteArrayOutputBuffer msgBuffer) {
    this.messageValueFactory = messageValueFactory;
    this.pointers = pointers;
    this.msgBuffer = msgBuffer;
    // TODO - if needed implement same for Safe as well
    messageReader = new UnsafeReusableByteArrayInput();
  }

  /**
   * Create message from factory
   *
   * @return message instance
   */
  protected M createMessage() {
    return messageValueFactory.newInstance();
  }

  @Override
  public Iterator<M> iterator() {
    return new Iterator<M>() {
      private final LongListIterator iterator = pointers.iterator();
      private final M reusableMsg =
        PointerListMessagesIterable.this.createMessage();
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public M next() {
        long pointer = iterator.nextLong();
        try {
          int index = (int) (pointer >>> 32);
          int offset = (int) pointer;
          ExtendedDataOutput buffer = msgBuffer.getDataOutput(index);
          messageReader.initialize(buffer.getByteArray(), offset,
            buffer.getPos());
          reusableMsg.readFields(messageReader);
        } catch (IOException e) {
          throw new IllegalStateException("Got exception : " + e);
        }
        return reusableMsg;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
