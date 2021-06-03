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
package org.apache.giraph.block_app.framework.piece.delegate;

import java.util.ArrayList;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Piece which uses a provided suppliers to decide whether or not to run
 * receive/send piece part on a certain vertex.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 * @param <WV> Worker value type
 * @param <WM> Worker message type
 * @param <S> Execution stage type
 */
@SuppressWarnings({ "rawtypes" })
public class FilteringPiece<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable, WV, WM extends Writable, S>
    extends DelegatePiece<I, V, E, M, WV, WM, S> {
  private final SupplierFromVertex<I, V, E, Boolean> toCallSend;
  private final SupplierFromVertex<I, V, E, Boolean> toCallReceive;

  /**
   * Creates filtering piece which uses passed {@code toCallSend} to filter
   * calls to {@code vertexSend}, and passed {@code toCallReceive} to filter
   * calls to {@code vertexReceive}, on passed {@code innerPiece}.
   */
  @SuppressWarnings("unchecked")
  public FilteringPiece(
      SupplierFromVertex<? super I, ? super V, ? super E, Boolean> toCallSend,
      SupplierFromVertex<? super I, ? super V, ? super E, Boolean>
        toCallReceive,
      AbstractPiece<? super I, ? super V, ? super E, ? super M,
        ? super WV, ? super WM, ? super S> innerPiece) {
    super(innerPiece);
    // Suppliers are contravariant on vertex types,
    // but Java generics cannot express that,
    // so use unchecked cast inside to allow callers to be typesafe
    this.toCallSend = (SupplierFromVertex) toCallSend;
    this.toCallReceive = (SupplierFromVertex) toCallReceive;
    Preconditions.checkArgument(
        toCallSend != null || toCallReceive != null,
        "Both send and receive filter cannot be null");
  }

  /**
   * Creates filtering piece, where both vertexSend and vertexReceive is
   * filtered based on same supplier.
   */
  public FilteringPiece(
      SupplierFromVertex<? super I, ? super V, ? super E, Boolean>
        toCallSendAndReceive,
      AbstractPiece<? super I, ? super V, ? super E, ? super M,
        ? super WV, ? super WM, ? super S> innerPiece) {
    this(toCallSendAndReceive, toCallSendAndReceive, innerPiece);
  }

  /**
   * Creates filtering piece, that filters only vertexReceive function,
   * and always calls vertexSend function.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable, WV, WM extends Writable, S>
  FilteringPiece<I, V, E, M, WV, WM, S> createReceiveFiltering(
      SupplierFromVertex<? super I, ? super V, ? super E, Boolean>
        toCallReceive,
      AbstractPiece<? super I, ? super V, ? super E, ? super M,
        ? super WV, ? super WM, ? super S> innerPiece) {
    return new FilteringPiece<>(null, toCallReceive, innerPiece);
  }

  /**
   * Creates filtering block, that filters only vertexSend function,
   * and always calls vertexReceive function.
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, M extends Writable, WV, WM extends Writable, S>
  FilteringPiece<I, V, E, M, WV, WM, S> createSendFiltering(
      SupplierFromVertex<? super I, ? super V, ? super E, Boolean> toCallSend,
      AbstractPiece<? super I, ? super V, ? super E, ? super M, ? super WV,
        ? super WM, ? super S> innerPiece) {
    return new FilteringPiece<>(toCallSend, null, innerPiece);
  }

  @Override
  protected DelegateWorkerSendFunctions delegateWorkerSendFunctions(
      ArrayList<InnerVertexSender> workerSendFunctions,
      BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage) {
    return new DelegateWorkerSendFunctions(workerSendFunctions) {
      @Override
      public void vertexSend(Vertex<I, V, E> vertex) {
        if (toCallSend == null || toCallSend.get(vertex)) {
          super.vertexSend(vertex);
        }
      }
    };
  }

  @Override
  protected DelegateWorkerReceiveFunctions delegateWorkerReceiveFunctions(
      ArrayList<VertexReceiver<I, V, E, M>> workerReceiveFunctions,
      BlockWorkerReceiveApi<I> workerApi, S executionStage) {
    return new DelegateWorkerReceiveFunctions(workerReceiveFunctions) {
      @Override
      public void vertexReceive(Vertex<I, V, E> vertex, Iterable<M> messages) {
        if (toCallReceive == null || toCallReceive.get(vertex)) {
          super.vertexReceive(vertex, messages);
        }
      }
    };
  }

  @Override
  protected String delegationName() {
    if (toCallSend != null && toCallReceive != null) {
      if (toCallSend != toCallReceive) {
        return "AsymFilter";
      }
      return "Filter";
    } else if (toCallSend != null) {
      return "SendFilter";
    } else if (toCallReceive != null) {
      return "ReceiveFilter";
    } else {
      throw new IllegalStateException("Both Send and Receive filters are null");
    }
  }
}
