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

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.AbstractPiece.InnerVertexSender;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexPostprocessor;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.graph.Vertex;

/**
 * Block execution logic on workers.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class BlockWorkerLogic {
  private final BlockWorkerPieces pieces;

  private transient VertexReceiver receiveFunctions;
  private transient InnerVertexSender sendFunctions;

  public BlockWorkerLogic(BlockWorkerPieces pieces) {
    this.pieces = pieces;
  }

  public void preSuperstep(
      BlockWorkerReceiveApi receiveApi, BlockWorkerSendApi sendApi) {
    pieces.getBlockApiHandle().setWorkerReceiveApi(receiveApi);
    pieces.getBlockApiHandle().setWorkerSendApi(sendApi);
    if (pieces.getReceiver() != null) {
      receiveFunctions = pieces.getReceiver().getVertexReceiver(receiveApi);
    }
    if (pieces.getSender() != null) {
      sendFunctions = pieces.getSender().getVertexSender(sendApi);
    }
  }

  public void compute(Vertex vertex, Iterable messages) {
    if (receiveFunctions != null) {
      receiveFunctions.vertexReceive(vertex, messages);
    }
    if (sendFunctions != null) {
      sendFunctions.vertexSend(vertex);
    }
  }

  public void postSuperstep() {
    if (receiveFunctions instanceof VertexPostprocessor) {
      ((VertexPostprocessor) receiveFunctions).postprocess();
    }
    if (sendFunctions != null) {
      sendFunctions.postprocess();
    }
  }
}
