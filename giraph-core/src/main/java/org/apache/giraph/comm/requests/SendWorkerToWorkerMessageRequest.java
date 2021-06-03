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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Request which sends any Writable message from one worker to another */
public class SendWorkerToWorkerMessageRequest extends WritableRequest
    implements WorkerRequest<WritableComparable, Writable, Writable> {
  /** Message sent */
  private Writable message;

  /**
   * Default constructor, for reflection
   */
  public SendWorkerToWorkerMessageRequest() {
  }

  /**
   * Constructor with message
   *
   * @param message Message sent
   */
  public SendWorkerToWorkerMessageRequest(Writable message) {
    this.message = message;
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_TO_WORKER_MESSAGE_REQUEST;
  }

  @Override
  void writeRequest(DataOutput output) throws IOException {
    Text.writeString(output, message.getClass().getName());
    message.write(output);
  }

  @Override
  void readFieldsRequest(DataInput input) throws IOException {
    String className = Text.readString(input);
    try {
      message = (Writable) Class.forName(className).newInstance();
      message.readFields(input);
    } catch (InstantiationException | IllegalAccessException |
        ClassNotFoundException e) {
      throw new IllegalStateException(
          "readFieldsRequest: Exception occurred", e);
    }
  }

  @Override
  public void doRequest(
      ServerData<WritableComparable, Writable, Writable> serverData) {
    serverData.addIncomingWorkerToWorkerMessage(message);
  }
}
