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
import org.apache.giraph.comm.flow_control.CreditBasedFlowControl;
import org.apache.giraph.comm.flow_control.FlowControl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

/**
 * Send to a worker a signal to resume sending messages to sender worker. This
 * type of request is used in adaptive credit-based flow control, where a
 * worker (W) may assign credit value of 0 to some worker (U), so that U would
 * stop sending messages to W. Later on, W may want to notify U to continue
 * sending messages to W. Along with the resume signal, W also announces a new
 * credit value to U.
 */
public class SendResumeRequest extends WritableRequest
    implements WorkerRequest {
  /** credit value */
  private short credit;

  /** Constructor used for reflection only */
  public SendResumeRequest() { }

  /**
   * Constructor
   *
   * @param credit credit value
   */
  public SendResumeRequest(short credit) {
    checkState(credit > 0);
    this.credit = credit;
  }

  @Override
  public void doRequest(ServerData serverData) {
    FlowControl flowControl =
        serverData.getServiceWorker().getWorkerClient().getFlowControl();
    checkState(flowControl != null);
    ((CreditBasedFlowControl) flowControl).processResumeSignal(getClientId(),
        credit, getRequestId());
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_RESUME_REQUEST;
  }

  @Override
  void readFieldsRequest(DataInput input) throws IOException {
    credit = input.readShort();
  }

  @Override
  void writeRequest(DataOutput output) throws IOException {
    output.writeShort(credit);
  }
}
