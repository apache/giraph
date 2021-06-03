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

import org.apache.giraph.master.MasterGlobalCommHandler;
import org.apache.giraph.io.InputType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A request which workers will send to master to ask it to give them splits
 */
public class AskForInputSplitRequest extends WritableRequest
    implements MasterRequest {
  /** Type of split we are requesting */
  private InputType splitType;
  /** Task id of worker which requested the split */
  private int workerTaskId;
  /**
   * Whether this is the first split a thread is requesting,
   * or this request indicates that previously requested input split was done
   */
  private boolean isFirstSplit;

  /**
   * Constructor
   *
   * @param splitType Type of split we are requesting
   * @param workerTaskId Task id of worker which requested the split
   * @param isFirstSplit Whether this is the first split a thread is requesting,
   *   or this request indicates that previously requested input split was done
   */
  public AskForInputSplitRequest(InputType splitType, int workerTaskId,
      boolean isFirstSplit) {
    this.splitType = splitType;
    this.workerTaskId = workerTaskId;
    this.isFirstSplit = isFirstSplit;
  }

  /**
   * Constructor used for reflection only
   */
  public AskForInputSplitRequest() {
  }

  @Override
  public void doRequest(MasterGlobalCommHandler commHandler) {
    commHandler.getInputSplitsHandler().sendSplitTo(
        splitType, workerTaskId, isFirstSplit);
  }

  @Override
  void readFieldsRequest(DataInput in) throws IOException {
    splitType = InputType.values()[in.readInt()];
    workerTaskId = in.readInt();
    isFirstSplit = in.readBoolean();
  }

  @Override
  void writeRequest(DataOutput out) throws IOException {
    out.writeInt(splitType.ordinal());
    out.writeInt(workerTaskId);
    out.writeBoolean(isFirstSplit);
  }

  @Override
  public RequestType getType() {
    return RequestType.ASK_FOR_INPUT_SPLIT_REQUEST;
  }
}
