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
package org.apache.giraph.block_app.framework.piece;

import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Piece that should be extended in common usecases, when we want to be:
 * - sending and then receiving messages from vertices
 * - sending data to be aggregated from workers to master
 * - sending values from master, via aggregators, to workers
 *
 * (basically - we don't want to use WorkerContext)
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 * @param <S> Execution stage type
 */
@SuppressWarnings("rawtypes")
public class Piece<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable, S>
    extends DefaultParentPiece<I, V, E, M, Object, NoMessage, S> {

  // Disallowing use of Worker Context functions:
  @Override
  public final void workerContextSend(
      BlockWorkerContextSendApi<I, NoMessage> workerContextApi,
      S executionStage, Object workerValue) {
  }

  @Override
  public final void workerContextReceive(
      BlockWorkerContextReceiveApi workerContextApi,
      S executionStage, Object workerValue, List<NoMessage> workerMessages) {
  }
}
