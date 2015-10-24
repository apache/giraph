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
package org.apache.giraph.block_app.migration;

import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.PieceWithWorkerContext;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.migration.MigrationAbstractComputation.MigrationFullAbstractComputation;
import org.apache.giraph.block_app.migration.MigrationMasterCompute.MigrationFullMasterCompute;
import org.apache.giraph.block_app.migration.MigrationWorkerContext.MigrationFullWorkerContext;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.DefaultMessageClasses;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;


/**
 * Piece used when migrating applications to Blocks Framework.
 *
 * There are two migration levels:
 * <ul>
 * <li>
 * drop-in replacement migration is completely compatible with previous code.
 * Only necessary thing is to change parent classes from (AbstractComputation,
 * MasterCompute, WorkerContext) to (MigrationFullAbstractComputation,
 * MigrationFullMasterCompute and MigrationFullWorkerContext).
 * After that, all you need to do is extend MigrationBlockFactory, and pass
 * appropriate types and call createMigrationAppBlock with initial computations.
 * <br>
 * You can now combine multiple applications, or use any library written in the
 * framework, but your application is left as one whole indivisible block.
 * </li>
 * <li>
 * Piece-wise migration - which gives a set of independent pieces, which can
 * then be combined with appropriate ordering logic within a BlockFactory.
 * You need to modify parent classes in your code to
 * (MigrationAbstractComputation, MigrationMasterCompute and
 * MigrationWorkerContext), which don't have any methods that affect computation
 *  ordering - and so calling those methods should be
 * moved to logic within BlockFactory.
 * Calling MigrationPiece.createMigrationPiece and passing appropriate
 * computations, gives you an independent piece, that you can then use in the
 * same way as before, but also combine it in any other way with other pieces
 * you have or are written within a library.
 * </li>
 * </ul>
 *
 * Generally, migration path can be to first move to drop-in replacement without
 * any effort, and then see which parts need to be modified to be able to use
 * piece-wise migration. At the end, it should be trivial to move from
 * piece-wise migration to directly using pieces, by just moving code around,
 * if you want to.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <MPrev> Previous piece message type
 * @param <M> Message type
 */
@SuppressWarnings("rawtypes")
public final class MigrationPiece<I extends WritableComparable,
    V extends Writable, E extends Writable, MPrev extends Writable,
    M extends Writable> extends PieceWithWorkerContext<I, V, E, M,
    MigrationWorkerContext, Writable, MigrationSuperstepStage> {

  private final Class<? extends MigrationAbstractComputation<I, V, E, MPrev, M>>
  computationClass;

  private final transient MigrationMasterCompute masterCompute;
  private final Supplier<Iterable<MPrev>> previousMessagesSupplier;
  private final Consumer<Iterable<M>> currentMessagesConsumer;
  private final transient Class<M> messageClass;
  private final transient Class<? extends MessageCombiner<? super I, M>>
  messageCombinerClass;

  private final boolean isFullMigration;
  private final boolean isFirstStep;

  private transient MigrationPiece nextPiece;
  private boolean isHalted;

  private MigrationPiece(
      Class<? extends MigrationAbstractComputation<I, V, E, MPrev, M>>
        computationClass,
      MigrationMasterCompute masterCompute, Supplier<Iterable<MPrev>>
        previousMessagesSupplier,
      Consumer<Iterable<M>> currentMessagesConsumer, Class<M> messageClass,
      Class<? extends MessageCombiner<? super I, M>> messageCombinerClass,
      boolean isFullMigration, boolean isFirstStep) {
    this.computationClass = computationClass;
    this.masterCompute = masterCompute;
    this.previousMessagesSupplier = previousMessagesSupplier;
    this.currentMessagesConsumer = currentMessagesConsumer;
    this.messageClass = messageClass;
    this.messageCombinerClass = messageCombinerClass;
    this.isFullMigration = isFullMigration;
    this.isFirstStep = isFirstStep;
    isHalted = false;
    nextPiece = null;
    sanityChecks();
  }


  @SuppressWarnings("unchecked")
  static <I extends WritableComparable, V extends Writable, E extends Writable,
  MR extends Writable, MS extends Writable>
  MigrationPiece<I, V, E, MR, MS> createFirstFullMigrationPiece(
      Class<? extends MigrationAbstractComputation<I, V, E, MR, MS>>
        computationClass,
      MigrationFullMasterCompute masterCompute,
      Class<MS> messageClass,
      Class<? extends MessageCombiner<? super I, MS>> messageCombinerClass) {
    ObjectTransfer transfer = new ObjectTransfer();
    return new MigrationPiece<>(
        computationClass, masterCompute, transfer, transfer, messageClass,
        messageCombinerClass,
        true, true);
  }

  public static <I extends WritableComparable, V extends Writable,
  E extends Writable, MR extends Writable, MS extends Writable>
  MigrationPiece<I, V, E, MR, MS> createMigrationPiece(
      Class<? extends MigrationAbstractComputation<I, V, E, MR, MS>>
        computationClass,
      MigrationMasterCompute masterCompute,
      Supplier<Iterable<MR>> previousMessagesSupplier,
      Consumer<Iterable<MS>> currentMessagesConsumer,
      Class<MS> messageClass,
      Class<? extends MessageCombiner<? super I, MS>> messageCombinerClass) {
    return new MigrationPiece<>(
        computationClass, masterCompute, previousMessagesSupplier,
        currentMessagesConsumer, messageClass, messageCombinerClass,
        false, false);
  }


  private void sanityChecks() {
    Preconditions.checkState(isFullMigration ==
        MigrationFullAbstractComputation.class
          .isAssignableFrom(computationClass));
  }

  void sanityTypeChecks(
      GiraphConfiguration conf, Class<?> previousMessageClass) {
    if (computationClass != null) {
      final Class<?> vertexIdClass = GiraphConstants.VERTEX_ID_CLASS.get(conf);
      final Class<?> vertexValueClass =
          GiraphConstants.VERTEX_VALUE_CLASS.get(conf);
      final Class<?> edgeValueClass =
          GiraphConstants.EDGE_VALUE_CLASS.get(conf);

      Class<?>[] classList = getTypeArguments(
          TypesHolder.class, computationClass);
      Preconditions.checkArgument(classList.length == 5);

      ReflectionUtils.verifyTypes(
          vertexIdClass, classList[0], "vertexId", computationClass);
      ReflectionUtils.verifyTypes(
          vertexValueClass, classList[1], "vertexValue", computationClass);
      ReflectionUtils.verifyTypes(
          edgeValueClass, classList[2], "edgeValue", computationClass);
      if (previousMessageClass != null) {
        ReflectionUtils.verifyTypes(
            previousMessageClass, classList[3], "recvMessage",
            computationClass);
      }
      ReflectionUtils.verifyTypes(
          messageClass, classList[4], "sendMessage", computationClass);
    }
  }

  @Override
  public void registerAggregators(BlockMasterApi masterApi)
      throws InstantiationException, IllegalAccessException {
    if (masterCompute != null) {
      masterCompute.init(masterApi);
      masterCompute.initialize();
    }
  }

  @Override
  public VertexSender<I, V, E> getVertexSender(
      BlockWorkerSendApi<I, V, E, M> workerApi,
      MigrationSuperstepStage executionStage) {
    if (computationClass == null || isFirstStep) {
      return null;
    }

    final MigrationAbstractComputation<I, V, E, MPrev, M> computation =
        ReflectionUtils.newInstance(computationClass);
    computation.init(
        workerApi, getWorkerValue(workerApi),
        executionStage.getMigrationSuperstep() - 1);
    computation.preSuperstep();

    return new InnerVertexSender() {
      @Override
      public void vertexSend(Vertex<I, V, E> vertex) {
        try {
          Iterable<MPrev> messages = null;
          if (previousMessagesSupplier != null) {
            messages = previousMessagesSupplier.get();
          }
          if (messages == null) {
            messages = Collections.<MPrev>emptyList();
          }
          computation.compute(vertex, messages);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void postprocess() {
        computation.postSuperstep();
      }
    };
  }

  @Override
  public void workerContextSend(
      BlockWorkerContextSendApi<I, Writable> workerContextApi,
      MigrationSuperstepStage executionStage,
      MigrationWorkerContext workerValue) {
    if (workerValue != null && !isFirstStep) {
      workerValue.setApi(workerContextApi);
      workerValue.postSuperstep();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void masterCompute(BlockMasterApi masterApi,
      MigrationSuperstepStage executionStage) {
    MigrationFullMasterCompute masterComputeF =
        isFullMigration ? (MigrationFullMasterCompute) masterCompute : null;

    if (masterCompute != null) {
      masterCompute.init(masterApi);

      if (masterComputeF != null) {
        masterComputeF.init(
            executionStage.getMigrationSuperstep(),
            computationClass, messageClass, messageCombinerClass);
      }

      masterCompute.compute();
    }

    if (isFullMigration) {
      if (masterComputeF != null) {
        isHalted = masterComputeF.isHalted();
        if (masterComputeF.isHalted()) {
          nextPiece = null;
        } else {
          if (masterComputeF.getNewComputationClass() != null ||
              masterComputeF.getNewMessage() != null ||
                  masterComputeF.getNewMessageCombiner() != null) {
            nextPiece = new MigrationPiece(
                masterComputeF.getComputationClass(),
                masterComputeF,
                previousMessagesSupplier,
                currentMessagesConsumer,
                masterComputeF.getOutgoingMessage(),
                masterComputeF.getMessageCombiner(),
                true, false);
          } else {
            nextPiece = this;
          }
        }
      } else {
        nextPiece = this;
      }
      if (nextPiece != null) {
        if (nextPiece.isFirstStep) {
          nextPiece = new MigrationPiece<>(
              computationClass,
              masterComputeF,
              previousMessagesSupplier,
              currentMessagesConsumer,
              messageClass,
              messageCombinerClass,
              true, false);
        }
        nextPiece.sanityTypeChecks(masterApi.getConf(), messageClass);
      }
    } else {
      Preconditions.checkState(!isHalted);
      Preconditions.checkState(nextPiece == null);
    }
  }

  @Override
  public void workerContextReceive(
      BlockWorkerContextReceiveApi workerContextApi,
      MigrationSuperstepStage executionStage,
      MigrationWorkerContext workerValue, List<Writable> workerMessages) {
    if (workerValue != null) {
      workerValue.setApi(workerContextApi);
      workerValue.setReceivedMessages(workerMessages);

      if (isFirstStep && workerValue instanceof MigrationFullWorkerContext) {
        try {
          ((MigrationFullWorkerContext) workerValue).preApplication();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      if (!isHalted) {
        workerValue.preSuperstep();
      }

      if (isHalted && workerValue instanceof MigrationFullWorkerContext) {
        ((MigrationFullWorkerContext) workerValue).postApplication();
      }
    }
  }

  @Override
  public VertexReceiver<I, V, E, M> getVertexReceiver(
      BlockWorkerReceiveApi<I> workerApi,
      MigrationSuperstepStage executionStage) {
    if (currentMessagesConsumer == null || isHalted) {
      return null;
    }

    return new InnerVertexReceiver() {
      @Override
      public void vertexReceive(Vertex<I, V, E> vertex, Iterable<M> messages) {
        currentMessagesConsumer.apply(messages);
      }
    };
  }

  @Override
  public MessageClasses<I, M> getMessageClasses(
      ImmutableClassesGiraphConfiguration conf) {
    return new DefaultMessageClasses(
        messageClass,
        DefaultMessageValueFactory.class,
        messageCombinerClass,
        GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE.get(conf));
  }

  @Override
  public MigrationSuperstepStage nextExecutionStage(
      MigrationSuperstepStage executionStage) {
    return executionStage.changedMigrationSuperstep(
        executionStage.getMigrationSuperstep() + 1);
  }

  public MigrationPiece getNextPiece() {
    Preconditions.checkState(isFullMigration);
    MigrationPiece res = nextPiece;
    nextPiece = null;
    return res;
  }

}
