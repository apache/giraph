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

import java.io.DataOutput;
import java.io.IOException;

/** Verbose Error mesage for ByteArray based messages */
public class VerboseByteStructMessageWrite {
  /**
   * Private Constructor
   */
  private VerboseByteStructMessageWrite() {
  }

  /**
   * verboseWriteCurrentMessage
   * de-serialize, then write messages
   *
   * @param iterator iterator
   * @param out DataOutput
   * @param <I> vertexId
   * @param <M> message
   * @throws IOException
   * @throws RuntimeException
   */
  public static <I extends WritableComparable, M extends Writable> void
  verboseWriteCurrentMessage(VertexIdMessageIterator<I, M> iterator,
    DataOutput out) throws IOException {
    try {
      iterator.getCurrentMessage().write(out);
    } catch (NegativeArraySizeException e) {
      handleNegativeArraySize(iterator.getCurrentVertexId());
    }
  }

  /**
   * message to present on NegativeArraySizeException
   *
   * @param vertexId vertexId
   * @param <I> vertexId type
   */
  public static <I extends WritableComparable> void handleNegativeArraySize(
      I vertexId) {
    throw new RuntimeException("The numbers of bytes sent to vertex " +
        vertexId + " exceeded the max capacity of " +
        "its ExtendedDataOutput. Please consider setting " +
        "giraph.useBigDataIOForMessages=true. If there are super-vertices" +
        " in the graph which receive a lot of messages (total serialized " +
        "size of messages goes beyond the maximum size of a byte array), " +
        "setting this option to true will remove that limit");
  }
}
