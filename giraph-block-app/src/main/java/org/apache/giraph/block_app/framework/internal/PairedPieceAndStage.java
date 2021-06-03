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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.AbstractPiece.InnerVertexSender;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.hadoop.io.Writable;

/**
 * Object holding piece with it's corresponding execution stage.
 *
 * @param <S> Execution stage type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class PairedPieceAndStage<S> {
  private final AbstractPiece piece;
  private final S executionStage;

  public PairedPieceAndStage(AbstractPiece piece, S executionStage) {
    this.piece = piece;
    this.executionStage = executionStage;
  }

  public S nextExecutionStage() {
    // if piece is null, then it cannot change the execution stage
    return piece != null ?
      (S) piece.nextExecutionStage(executionStage) : executionStage;
  }

  public S getExecutionStage() {
    return executionStage;
  }

  public void registerReducers(BlockMasterApi masterApi) {
    if (piece != null) {
      piece.wrappedRegisterReducers(masterApi, executionStage);
    }
  }

  public InnerVertexSender getVertexSender(BlockWorkerSendApi sendApi) {
    if (piece != null) {
      return piece.getWrappedVertexSender(sendApi, executionStage);
    }
    return null;
  }

  public void masterCompute(BlockMasterApi masterApi) {
    if (piece != null) {
      piece.masterCompute(masterApi, executionStage);
    }
  }

  public VertexReceiver getVertexReceiver(
      BlockWorkerReceiveApi receiveApi) {
    if (piece != null) {
      return piece.getVertexReceiver(receiveApi, executionStage);
    }
    return null;
  }

  public void workerContextSend(
      BlockWorkerContextSendApi workerContextApi, Object workerValue) {
    if (piece != null) {
      piece.workerContextSend(workerContextApi, executionStage, workerValue);
    }
  }

  public void workerContextReceive(
      BlockWorkerContextReceiveApi workerContextApi,
      Object workerValue, List<Writable> workerMessages) {
    if (piece != null) {
      piece.workerContextReceive(
          workerContextApi, executionStage, workerValue, workerMessages);
    }
  }

  /**
   * @return the piece
   */
  public AbstractPiece getPiece() {
    return piece;
  }

  @Override
  public String toString() {
    return "Piece " + piece + " in stage " + executionStage;
  }
}
