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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.flow_control.FlowControl;
import org.apache.giraph.comm.requests.MasterRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.master.MasterGlobalCommHandler;

/** Handler for requests on master */
public class MasterRequestServerHandler extends
    RequestServerHandler<MasterRequest> {
  /** Aggregator handler */
  private final MasterGlobalCommHandler commHandler;

  /**
   * Constructor
   *
   * @param workerRequestReservedMap Worker request reservation map
   * @param conf                     Configuration
   * @param myTaskInfo               Current task info
   * @param commHandler              Master communication handler
   * @param exceptionHandler         Handles uncaught exceptions
   * @param flowControl              Reference to the flow control used
   */
  public MasterRequestServerHandler(
      WorkerRequestReservedMap workerRequestReservedMap,
      ImmutableClassesGiraphConfiguration conf,
      TaskInfo myTaskInfo,
      MasterGlobalCommHandler commHandler,
      Thread.UncaughtExceptionHandler exceptionHandler,
      FlowControl flowControl) {
    super(workerRequestReservedMap, conf, myTaskInfo, exceptionHandler);
    this.commHandler = commHandler;
    this.flowControl = flowControl;
  }

  @Override
  public void processRequest(MasterRequest request) {
    request.doRequest(commHandler);
  }

  /**
   * Factory for {@link MasterRequestServerHandler}
   */
  public static class Factory implements RequestServerHandler.Factory {
    /** Master aggregator handler */
    private final MasterGlobalCommHandler commHandler;
    /** Flow control used in sending requests */
    private FlowControl flowControl;

    /**
     * Constructor
     *
     * @param commHandler Master global communication handler
     */
    public Factory(MasterGlobalCommHandler commHandler) {
      this.commHandler = commHandler;
    }

    @Override
    public RequestServerHandler newHandler(
        WorkerRequestReservedMap workerRequestReservedMap,
        ImmutableClassesGiraphConfiguration conf,
        TaskInfo myTaskInfo,
        Thread.UncaughtExceptionHandler exceptionHandler) {
      return new MasterRequestServerHandler(workerRequestReservedMap, conf,
          myTaskInfo, commHandler, exceptionHandler, flowControl);
    }

    @Override
    public void setFlowControl(FlowControl flowControl) {
      this.flowControl = flowControl;
    }
  }
}
