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
package org.apache.giraph.block_app.framework.internal;

import java.util.List;

import org.apache.giraph.block_app.framework.BlockUtils;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Block execution logic on WorkerContext.
 */
@SuppressWarnings({ "rawtypes" })
public class BlockWorkerContextLogic {
  public static final Logger LOG =
      Logger.getLogger(BlockWorkerContextLogic.class);

  private Object workerValue;
  private BlockWorkerPieces workerPieces;
  private BlockOutputHandle outputHandle;

  private transient BlockWorkerContextSendApi sendApi;

  public BlockWorkerContextLogic() {
  }

  public void preApplication(BlockWorkerContextApi api,
      BlockOutputHandle outputHandle) {
    workerValue =
        BlockUtils.BLOCK_WORKER_CONTEXT_VALUE_CLASS.newInstance(api.getConf());
    this.outputHandle = outputHandle;
  }

  public Object getWorkerValue() {
    return workerValue;
  }

  public BlockOutputHandle getOutputHandle() {
    return outputHandle;
  }

  @SuppressWarnings("unchecked")
  public void preSuperstep(
      BlockWorkerContextReceiveApi receiveApi,
      BlockWorkerContextSendApi sendApi,
      BlockWorkerPieces workerPieces, long superstep,
      List<Writable> messages) {
    workerPieces.getBlockApiHandle().setWorkerContextReceiveApi(receiveApi);
    workerPieces.getBlockApiHandle().setWorkerContextSendApi(sendApi);
    if (BlockUtils.LOG_EXECUTION_STATUS.get(receiveApi.getConf())) {
      LOG.info("Worker executing " + workerPieces + " in " + superstep +
          " superstep");
    }
    this.sendApi = sendApi;
    this.workerPieces = workerPieces;
    if (workerPieces.getReceiver() != null) {
      workerPieces.getReceiver().workerContextReceive(
          receiveApi, workerValue, messages);
    }
  }

  public void postSuperstep() {
    if (workerPieces.getSender() != null) {
      workerPieces.getSender().workerContextSend(sendApi, workerValue);
    }
    workerPieces = null;
    sendApi = null;
    outputHandle.returnAllWriters();
  }

  public void postApplication() {
    outputHandle.closeAllWriters();
    // TODO add support through conf for postApplication, if needed.
  }
}
