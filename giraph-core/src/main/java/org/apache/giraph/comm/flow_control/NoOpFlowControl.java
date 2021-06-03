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

package org.apache.giraph.comm.flow_control;

import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.handler.AckSignalFlag;
import org.apache.giraph.comm.requests.WritableRequest;

/**
 * Representation of a flow control policy that does not do anything other than
 * the vanilla network client request transfer mechanism
 */
public class NoOpFlowControl implements FlowControl {
  /** Netty client */
  private final NettyClient nettyClient;

  /**
   * Constructor
   *
   * @param nettyClient netty client
   */
  public NoOpFlowControl(NettyClient nettyClient) {
    this.nettyClient = nettyClient;
  }

  @Override
  public void sendRequest(int destTaskId, WritableRequest request) {
    nettyClient.doSend(destTaskId, request);
  }

  @Override
  public void messageAckReceived(int taskId, long requestId, int response) { }

  @Override
  public AckSignalFlag getAckSignalFlag(int response) {
    return AckSignalFlag.values()[response];
  }

  @Override
  public void waitAllRequests() { }

  @Override
  public int getNumberOfUnsentRequests() {
    return 0;
  }

  @Override
  public int calculateResponse(AckSignalFlag alreadyDone, int taskId) {
    return alreadyDone.ordinal();
  }

  @Override
  public void logInfo() { }
}
