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
package org.apache.giraph.block_app.framework.api;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class that contains references to Block Api objects.
 *
 * One general use-case for this is for applications to indirectly get a handle
 * on the Block Api objects and  implement operations that (i) depend on the
 * Block Api interfaces, (ii) are not in the context of a Piece when defined,
 * and (iii) are in the context of a Piece when executed.
 *
 * To do this, as opposed to defining an application as a
 * {@link org.apache.giraph.block_app.framework.block.Block}, define
 * your application as a
 * {@link org.apache.giraph.block_app.framework.block.BlockWithApiHandle}.
 *
 * NOTE: Depending on the context in which this class is used, some of the
 * handles may not be set. For instance, the {@link masterApi} is not set when
 * this is in the context of a worker. Trying to get access to a handle when
 * it is not set will result in a runtime exception. Instead, you should first
 * use methods like the {@link #isMasterApiSet()} to check.
 *
 * The *Api fields are transient as we do not need/want to serialize them. They
 * will be set at the appropriate time by the framework.
 */
public class BlockApiHandle {
  private transient BlockMasterApi masterApi;
  private transient BlockWorkerReceiveApi workerReceiveApi;
  private transient BlockWorkerSendApi workerSendApi;
  private transient BlockWorkerContextReceiveApi workerContextReceiveApi;
  private transient BlockWorkerContextSendApi workerContextSendApi;

  public void setMasterApi(BlockMasterApi api) {
    this.masterApi = api;
  }

  public void setWorkerReceiveApi(BlockWorkerReceiveApi api) {
    this.workerReceiveApi = api;
  }

  public void setWorkerSendApi(BlockWorkerSendApi api) {
    this.workerSendApi = api;
  }

  public void setWorkerContextReceiveApi(BlockWorkerContextReceiveApi api) {
    this.workerContextReceiveApi = api;
  }

  public void setWorkerContextSendApi(BlockWorkerContextSendApi api) {
    this.workerContextSendApi = api;
  }

  public boolean isMasterApiSet() {
    return masterApi != null;
  }

  public boolean isWorkerReceiveApiSet() {
    return workerReceiveApi != null;
  }

  public boolean isWorkerSendApiSet() {
    return workerSendApi != null;
  }

  public boolean isWorkerContextReceiveApiSet() {
    return workerContextReceiveApi != null;
  }

  public boolean isWorkerContextSendApiSet() {
    return workerContextSendApi != null;
  }

  public BlockMasterApi getMasterApi() {
    checkNotNull(masterApi,
      "BlockMasterApi not valid in this context.");
    return masterApi;
  }

  public BlockWorkerReceiveApi getWorkerReceiveApi() {
    checkNotNull(workerReceiveApi,
      "BlockWorkerReceiveApi not valid in this context.");
    return workerReceiveApi;
  }

  public BlockWorkerSendApi getWorkerSendApi() {
    checkNotNull(workerSendApi,
      "BlockWorkerSendApi not valid in this context.");
    return workerSendApi;
  }

  public BlockWorkerContextReceiveApi getWorkerContextReceiveApi() {
    checkNotNull(workerContextReceiveApi,
      "BlockWorkerContextReceiveApi not valid in this context");
    return workerContextReceiveApi;
  }

  public BlockWorkerContextSendApi getWorkerContextSendApi() {
    checkNotNull(workerContextSendApi,
      "BlockWorkerContextSendApi not valid in this context");
    return workerContextSendApi;
  }
}
