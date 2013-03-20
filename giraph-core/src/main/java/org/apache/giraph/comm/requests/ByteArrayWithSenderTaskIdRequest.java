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

package org.apache.giraph.comm.requests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract request which has a byte array and task id of the sender as its
 * data
 */
public abstract class ByteArrayWithSenderTaskIdRequest
    extends ByteArrayRequest {
  /** Task id of the sender of request */
  private int senderTaskId;

  /**
   * Constructor
   *
   * @param data Request data
   * @param senderTaskId Sender task id
   */
  public ByteArrayWithSenderTaskIdRequest(byte[] data, int senderTaskId) {
    super(data);
    this.senderTaskId = senderTaskId;
  }

  /**
   * Default constructor
   */
  public ByteArrayWithSenderTaskIdRequest() {
  }

  public int getSenderTaskId() {
    return senderTaskId;
  }

  @Override
  void writeRequest(DataOutput output) throws IOException {
    super.writeRequest(output);
    output.writeInt(senderTaskId);
  }

  @Override
  void readFieldsRequest(DataInput input) throws IOException {
    super.readFieldsRequest(input);
    senderTaskId = input.readInt();
  }

  @Override
  public int getSerializedSize() {
    return super.getSerializedSize() + 4;
  }
}
