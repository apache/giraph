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

import org.apache.giraph.comm.netty.handler.AckSignalFlag;
import org.apache.giraph.comm.requests.WritableRequest;

/**
 * Interface representing flow control policy in sending requests
 */
public interface FlowControl {
  /**
   * This method is called by a network client for all requests that should be
   * handled by a *remote* task. All these requests should be controlled and/or
   * monitored by the flow control policy. The flow control policy may choose to
   * temporarily hold off from sending to a particular remote task and keep the
   * request in some cache for later transfer. A flow control mechanism is free
   * to implement this method as blocking or non-blocking. Note that, a
   * flow-control policy should adhere to exactly-once semantic, meaning it
   * should always send one and only one copy of each request that should be
   * handled by a remote task.
   *
   * @param destTaskId id of the worker to send the request to
   * @param request request to send
   */
  void sendRequest(int destTaskId, WritableRequest request);

  /**
   * Notify the flow control policy that an open request is completed.
   *
   * @param taskId id of the task to which the open request is completed
   * @param requestId id of the open request which is completed
   * @param response the response heard from the client
   */
  void messageAckReceived(int taskId, long requestId, int response);

  /**
   * Decode the acknowledgement signal from the response after an open request
   * is completed
   *
   * @param response the response heard after completion of a request
   * @return the Acknowledgement signal decoded from the response
   */
  AckSignalFlag getAckSignalFlag(int response);

  /**
   * There may be requests in possession of the flow control mechanism, as the
   * mechanism controls whether a task should send a request or not.
   * Calling this method causes the caller to wait until all requests in
   * possession of the flow control mechanism are sent out.
   */
  void waitAllRequests();

  /**
   * @return number of unsent requests in possession of the flow control policy
   */
  int getNumberOfUnsentRequests();

  /**
   * Calculate/Build the response to piggyback with acknowledgement
   *
   * @param flag indicating the status of processing of the request (whether it
   *             was a new request or it was a duplicate)
   * @param taskId id of the task the acknowledgement is for
   * @return the response to piggyback along with the acknowledgement message
   */
  int calculateResponse(AckSignalFlag flag, int taskId);

  /**
   * Log the status of the flow control
   */
  void logInfo();
}
