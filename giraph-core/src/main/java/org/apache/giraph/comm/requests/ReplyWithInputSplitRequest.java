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

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.io.InputType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A request which master will send to workers to give them splits
 */
public class ReplyWithInputSplitRequest extends WritableRequest
    implements WorkerRequest {
  /** Type of input split */
  private InputType splitType;
  /** Serialized input split */
  private byte[] serializedInputSplit;

  /**
   * Constructor
   *
   * @param splitType Type of input split
   * @param serializedInputSplit Serialized input split
   */
  public ReplyWithInputSplitRequest(InputType splitType,
      byte[] serializedInputSplit) {
    this.splitType = splitType;
    this.serializedInputSplit = serializedInputSplit;
  }

  /**
   * Constructor used for reflection only
   */
  public ReplyWithInputSplitRequest() {
  }

  @Override
  void readFieldsRequest(DataInput in) throws IOException {
    splitType = InputType.values()[in.readInt()];
    int size = in.readInt();
    serializedInputSplit = new byte[size];
    in.readFully(serializedInputSplit);
  }

  @Override
  void writeRequest(DataOutput out) throws IOException {
    out.writeInt(splitType.ordinal());
    out.writeInt(serializedInputSplit.length);
    out.write(serializedInputSplit);
  }

  @Override
  public void doRequest(ServerData serverData) {
    serverData.getServiceWorker().getInputSplitsHandler().receivedInputSplit(
        splitType, serializedInputSplit);
  }

  @Override
  public RequestType getType() {
    return RequestType.REPLY_WITH_INPUT_SPLIT_REQUEST;
  }
}
