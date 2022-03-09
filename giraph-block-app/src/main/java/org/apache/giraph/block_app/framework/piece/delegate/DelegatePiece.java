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
import java.util.Arrays;
import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.PieceCount;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexPostprocessor;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Delegate Piece which allows combining multiple pieces in same iteration:
 * new DelegatePiece(new LogicPiece(), new StatsPiece())
 * You should be careful when doing so, since those pieces must not interact,
 * and only one can send messages.
 * Execution of any of the Piece methods by the framework is going to trigger
 * sequential execution of that method on all of the pieces this DelegatePiece
 * wraps. That means for example, getVertexSender is going to be called on all
 * pieces before masterCompute is called on all pieces, which is called before
 * getVertexReceiver on all pieces.
 *
 * Also, via overriding, it provides an abstract class for filtering. I.e. if
 * you want piece that filters out calls to masterCompute, you can have:
 * new FilterMasterPiece(new LogicPiece()),
 * with FilterMasterPiece extends DelegatePiece, and only overrides getMaster
 * function and DelegateMasterPiece class.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 * @param <WV> Worker value type
 * @param <WM> Worker message type
 * @param <S> Execution stage type
 */
@SuppressWarnings("rawtypes")
public class DelegatePiece<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable, WV, WM extends Writable, S>
    extends AbstractPiece<I, V, E, M, WV, WM, S> {

  private final List<AbstractPiece<I, V, E, M, WV, WM, S>> innerPieces;

  @SafeVarargs
  @SuppressWarnings("unchecked")
  public DelegatePiece(AbstractPiece<? super I, ? super V, ? super E,
      ? super M, ? super WV, ? super WM, ? super S>... innerPieces) {
    // Pieces are contravariant, but Java generics cannot express that,
    // so use unchecked cast inside to allow callers to be typesafe
    this.innerPieces = new ArrayList(Arrays.asList(innerPieces));
  }

  @SuppressWarnings("unchecked")
  public DelegatePiece(AbstractPiece<? super I, ? super V, ? super E,
      ? super M, ? super WV, ? super WM, ? super S> innerPiece) {
    // Pieces are contravariant, but Java generics cannot express that,
    // so use unchecked cast inside to allow callers to be typesafe
    this.innerPieces = new ArrayList(Arrays.asList(innerPiece));
  }

  protected DelegateWorkerSendFunctions delegateWorkerSendFunctions(
      ArrayList<InnerVertexSender> workerSendFunctions,
      BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage) {
    return new DelegateWorkerSendFunctions(workerSendFunctions);
  }

  protected DelegateWorkerReceiveFunctions delegateWorkerReceiveFunctions(
      ArrayList<VertexReceiver<I, V, E, M>> workerReceiveFunctions,
      BlockWorkerReceiveApi<I> workerApi, S executionStage) {
    return new DelegateWorkerReceiveFunctions(workerReceiveFunctions);
  }

  @Override
  public InnerVertexSender getWrappedVertexSender(
      BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage) {
    ArrayList<InnerVertexSender> workerSendFunctions =
        new ArrayList<>(innerPieces.size());
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      workerSendFunctions.add(
          innerPiece.getWrappedVertexSender(workerApi, executionStage));
    }
    return delegateWorkerSendFunctions(
        workerSendFunctions, workerApi, executionStage);
  }

  @Override
  public InnerVertexReceiver getVertexReceiver(
      BlockWorkerReceiveApi<I> workerApi, S executionStage) {
    ArrayList<VertexReceiver<I, V, E, M>> workerReceiveFunctions =
        new ArrayList<>(innerPieces.size());
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      workerReceiveFunctions.add(
          innerPiece.getVertexReceiver(workerApi, executionStage));
    }
    return delegateWorkerReceiveFunctions(
        workerReceiveFunctions, workerApi, executionStage);
  }

  /** Delegating WorkerSendPiece */
  protected class DelegateWorkerSendFunctions extends InnerVertexSender {
    private final ArrayList<InnerVertexSender> workerSendFunctions;

    public DelegateWorkerSendFunctions(
        ArrayList<InnerVertexSender> workerSendFunctions) {
      this.workerSendFunctions = workerSendFunctions;
    }

    @Override
    public void vertexSend(Vertex<I, V, E> vertex) {
      for (InnerVertexSender functions : workerSendFunctions) {
        if (functions != null) {
          functions.vertexSend(vertex);
        }
      }
    }

    @Override
    public void postprocess() {
      for (InnerVertexSender functions : workerSendFunctions) {
        if (functions != null) {
          functions.postprocess();
        }
      }
    }
  }

  /** Delegating WorkerReceivePiece */
  protected class DelegateWorkerReceiveFunctions extends InnerVertexReceiver {
    private final ArrayList<VertexReceiver<I, V, E, M>>
    workerReceiveFunctions;

    public DelegateWorkerReceiveFunctions(
        ArrayList<VertexReceiver<I, V, E, M>> workerReceiveFunctions) {
      this.workerReceiveFunctions = workerReceiveFunctions;
    }

    @Override
    public void vertexReceive(Vertex<I, V, E> vertex, Iterable<M> messages) {
      for (VertexReceiver<I, V, E, M> functions :
            workerReceiveFunctions) {
        if (functions != null) {
          functions.vertexReceive(vertex, messages);
        }
      }
    }

    @Override
    public void postprocess() {
      for (VertexReceiver<I, V, E, M> functions :
            workerReceiveFunctions) {
        if (functions instanceof VertexPostprocessor) {
          ((VertexPostprocessor) functions).postprocess();
        }
      }
    }
  }

  @Override
  public void masterCompute(BlockMasterApi api, S executionStage) {
    for (AbstractPiece<I, V, E, M, WV, WM, S> piece : innerPieces) {
      piece.masterCompute(api, executionStage);
    }
  }

  @Override
  public void workerContextSend(
      BlockWorkerContextSendApi<I, WM> workerContextApi, S executionStage,
      WV workerValue) {
    for (AbstractPiece<I, V, E, M, WV, WM, S> piece : innerPieces) {
      piece.workerContextSend(workerContextApi, executionStage, workerValue);
    }
  }

  @Override
  public void workerContextReceive(
      BlockWorkerContextReceiveApi workerContextApi, S executionStage,
      WV workerValue, List<WM> workerMessages) {
    for (AbstractPiece<I, V, E, M, WV, WM, S> piece : innerPieces) {
      piece.workerContextReceive(
          workerContextApi, executionStage, workerValue, workerMessages);
    }
  }

  @Override
  public S nextExecutionStage(S executionStage) {
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      executionStage = innerPiece.nextExecutionStage(executionStage);
    }
    return executionStage;
  }

  @Override
  public MessageClasses<I, M> getMessageClasses(
      ImmutableClassesGiraphConfiguration conf) {
    MessageClasses<I, M> messageClasses = null;
    MessageClasses<I, M> firstMessageClasses = null;
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      MessageClasses<I, M> cur = innerPiece.getMessageClasses(conf);
      Preconditions.checkState(cur != null);
      if (!cur.getMessageClass().equals(NoMessage.class)) {
        if (messageClasses != null) {
          throw new RuntimeException(
              "Only one piece combined through delegate (" +
              toString() + ") can send messages");
        }
        messageClasses = cur;
      }
      if (firstMessageClasses == null) {
        firstMessageClasses = cur;
      }
    }
    return messageClasses != null ? messageClasses : firstMessageClasses;
  }

  @Override
  public void forAllPossiblePieces(Consumer<AbstractPiece> consumer) {
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      innerPiece.forAllPossiblePieces(consumer);
    }
  }

  @Override
  public PieceCount getPieceCount() {
    return new PieceCount(1);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void registerAggregators(BlockMasterApi master)
      throws InstantiationException, IllegalAccessException {
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      innerPiece.registerAggregators(master);
    }
  }

  @Override
  public void wrappedRegisterReducers(
      BlockMasterApi masterApi, S executionStage) {
    for (AbstractPiece<I, V, E, M, WV, WM, S> innerPiece : innerPieces) {
      innerPiece.wrappedRegisterReducers(masterApi, executionStage);
    }
  }

  protected String delegationName() {
    return "Delegate";
  }

  @Override
  public String toString() {
    return delegationName() + innerPieces.toString();
  }
}
