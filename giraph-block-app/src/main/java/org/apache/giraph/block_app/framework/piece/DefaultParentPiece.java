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

import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.piece.global_comm.ReduceUtilsObject;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.internal.CreateReducersApiWrapper;
import org.apache.giraph.block_app.framework.piece.global_comm.internal.ReducersForPieceHandler;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexPostprocessor;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.framework.piece.messages.ObjectMessageClasses;
import org.apache.giraph.block_app.framework.piece.messages.SupplierFromConf;
import org.apache.giraph.block_app.framework.piece.messages.SupplierFromConf.DefaultMessageFactorySupplierFromConf;
import org.apache.giraph.block_app.framework.piece.messages.SupplierFromConf.SupplierFromConfByCopy;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.EnumConfOption;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.types.NoMessage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Additional abstract implementations for all pieces to be used.
 * Code here is not in AbstractPiece only to allow for non-standard
 * non-user-defined pieces. <br>
 * Only logic used by the underlying framework directly is in AbstractPiece
 * itself.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 * @param <M> Message type
 * @param <WV> Worker value type
 * @param <WM> Worker message type
 * @param <S> Execution stage type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class DefaultParentPiece<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable, WV,
    WM extends Writable, S> extends AbstractPiece<I, V, E, M, WV, WM, S> {
  // TODO move to GiraphConstants
  /**
   * This option will tell which message encode &amp; store enum to force,
   * when combining is not enabled.
   *
   * MESSAGE_ENCODE_AND_STORE_TYPE and this property are basically upper
   * and lower bound on message store type, when looking them in order from
   * not doing anything special, to most advanced type:
   * BYTEARRAY_PER_PARTITION,
   * EXTRACT_BYTEARRAY_PER_PARTITION,
   * POINTER_LIST_PER_VERTEX
   * resulting encode type is going to be:
   * pieceEncodingType = piece.allowOneMessageToManyIdsEncoding() ?
   *    POINTER_LIST_PER_VERTEX : BYTEARRAY_PER_PARTITION)
   * Math.max(index(minForce), Math.min(index(maxAllowed), index(pieceType);
   *
   * This is useful to force all pieces onto particular message store, even
   * if they do not overrideallowOneMessageToManyIdsEncoding, though that might
   * be rarely needed.
   * This option might be more useful for fully local computation,
   * where overall job behavior is quite different.
   */
  public static final EnumConfOption<MessageEncodeAndStoreType>
  MESSAGE_ENCODE_AND_STORE_TYPE_MIN_FORCE =
      EnumConfOption.create("giraph.messageEncodeAndStoreTypeMinForce",
          MessageEncodeAndStoreType.class,
          MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION,
          "Select the message_encode_and_store_type min force to use");

  private final ReduceUtilsObject reduceUtils = new ReduceUtilsObject();
  private ReducersForPieceHandler reducersHandler;

  // Overridable functions

  /**
   * Override to register any potential reducers used by this piece,
   * through calls to {@code reduceApi}, which will return reducer handles
   * for simple.
   * Tip: Without defining a field, first write here name of the field and what
   * you want to reduce, like:
   * {@code totalSum = reduceApi.createLocalReducer(SumReduce.DOUBLE); }
   * and then use tools your IDE provides to generate field signature itself,
   * which might be slightly complex:
   * {@code ReducerHandle<DoubleWritable, DoubleWritable> totalSum; }
   */
  public void registerReducers(CreateReducersApi reduceApi, S executionStage) {
  }

  /**
   * Override to do vertex send processing.
   *
   * Creates handler that defines what should be executed on worker
   * during send phase.
   *
   * This logic gets executed first.
   * This function is called once on each worker on each thread, in parallel,
   * on their copy of Piece object to create functions handler.
   *
   * If returned object implements Postprocessor interface, then corresponding
   * postprocess() function is going to be called once, after all vertices
   * corresponding thread needed to process are done.
   */
  public VertexSender<I, V, E> getVertexSender(
      BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage) {
    return null;
  }

  /**
   * Override to specify type of the message this Piece sends, if it does
   * send messages.
   *
   * If not overwritten, no messages can be sent.
   */
  protected Class<M> getMessageClass() {
    return null;
  }

  /**
   * Override to specify message value factory to be used,
   * which creates objects into which messages will be deserialized.
   *
   * If not overwritten, or null is returned, DefaultMessageValueFactory
   * will be used.
   */
  protected MessageValueFactory<M> getMessageFactory(
      ImmutableClassesGiraphConfiguration conf) {
    return null;
  }

  /**
   * Override to specify message combiner to be used, if any.
   *
   * Message combiner itself should be immutable
   * (i.e. it will be call simultanously from multiple threads)
   */
  protected MessageCombiner<? super I, M> getMessageCombiner(
      ImmutableClassesGiraphConfiguration conf) {
    return null;
  }

  /**
   * Override to specify that this Piece allows one to many ids encoding to be
   * used for messages.
   * You should override this function, if you are sending identical message to
   * all targets, and message itself is not extremely small.
   */
  protected boolean allowOneMessageToManyIdsEncoding() {
    return false;
  }

  /**
   * Override to specify that receive of this Piece (and send of next Piece)
   * ignore existing vertices, and just process received messages.
   *
   * Useful when distributed processing on groups that are not vertices is
   * needed. This flag allows you not to worry whether a destination vertex
   * exist, and removes need to clean it up when finished.
   * One example is if each vertex is in a cluster, and we need to process
   * something per cluster.
   *
   * Alternative are reducers, which have distributed reduction, but mostly
   * master still does the processing afterwards, and amount of data needs to
   * fit single machine (master).
   */
  protected boolean receiveIgnoreExistingVertices() {
    return false;
  }

  @Override
  public MessageClasses<I, M> getMessageClasses(
      ImmutableClassesGiraphConfiguration conf) {
    Class<M> messageClass = null;
    MessageValueFactory<M> messageFactory = getMessageFactory(conf);
    MessageCombiner<? super I, M> messageCombiner = getMessageCombiner(conf);

    if (messageFactory != null) {
      messageClass = (Class) messageFactory.newInstance().getClass();
    } else if (messageCombiner != null) {
      messageClass = (Class) messageCombiner.createInitialMessage().getClass();
    }

    if (messageClass != null) {
      Preconditions.checkState(getMessageClass() == null,
          "Piece %s defines getMessageFactory or getMessageCombiner, " +
          "so it doesn't need to define getMessageClass.",
          toString());
    } else {
      messageClass = getMessageClass();
      if (messageClass == null) {
        messageClass = (Class) NoMessage.class;
      }
    }

    SupplierFromConf<MessageValueFactory<M>> messageFactorySupplier;
    if (messageFactory != null) {
      messageFactorySupplier =
          new SupplierFromConfByCopy<MessageValueFactory<M>>(messageFactory);
    } else {
      messageFactorySupplier =
          new DefaultMessageFactorySupplierFromConf<>(messageClass);
    }

    SupplierFromConf<? extends MessageCombiner<? super I, M>>
    messageCombinerSupplier;
    if (messageCombiner != null) {
      messageCombinerSupplier = new SupplierFromConfByCopy<>(messageCombiner);
    } else {
      messageCombinerSupplier = null;
    }

    int maxAllowed =
        GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE.get(conf).ordinal();
    int minForce = MESSAGE_ENCODE_AND_STORE_TYPE_MIN_FORCE.get(conf).ordinal();
    Preconditions.checkState(maxAllowed >= minForce);

    int pieceEncodeType = (allowOneMessageToManyIdsEncoding() ?
        MessageEncodeAndStoreType.POINTER_LIST_PER_VERTEX :
        MessageEncodeAndStoreType.BYTEARRAY_PER_PARTITION).ordinal();
    // bound piece type with boundaries:
    pieceEncodeType = Math.max(minForce, Math.min(maxAllowed, pieceEncodeType));

    MessageEncodeAndStoreType messageEncodeAndStoreType =
        MessageEncodeAndStoreType.values()[pieceEncodeType];

    if (messageFactory instanceof GiraphConfigurationSettable) {
      throw new IllegalStateException(
          messageFactory.getClass() + " MessageFactory in " + this +
          " Piece implements GiraphConfigurationSettable");
    }
    if (messageCombiner instanceof GiraphConfigurationSettable) {
      throw new IllegalStateException(
          messageCombiner.getClass() + " MessageCombiner in " + this +
          " Piece implements GiraphConfigurationSettable");
    }

    return new ObjectMessageClasses<>(
        messageClass, messageFactorySupplier,
        messageCombinerSupplier, messageEncodeAndStoreType,
        receiveIgnoreExistingVertices());
  }

  // Internal implementation

  @Override
  public final InnerVertexSender getWrappedVertexSender(
      final BlockWorkerSendApi<I, V, E, M> workerApi, S executionStage) {
    reducersHandler.vertexSenderWorkerPreprocess(workerApi);
    final VertexSender<I, V, E> functions =
        getVertexSender(workerApi, executionStage);
    return new InnerVertexSender() {
      @Override
      public void vertexSend(Vertex<I, V, E> vertex) {
        if (functions != null) {
          functions.vertexSend(vertex);
        }
      }
      @Override
      public void postprocess() {
        if (functions instanceof VertexPostprocessor) {
          ((VertexPostprocessor) functions).postprocess();
        }
        reducersHandler.vertexSenderWorkerPostprocess(workerApi);
      }
    };
  }

  @Override
  public final void wrappedRegisterReducers(
      BlockMasterApi masterApi, S executionStage) {
    reducersHandler = new ReducersForPieceHandler();
    registerReducers(new CreateReducersApiWrapper(
        masterApi, reducersHandler), executionStage);
  }

  // utility functions:
  // TODO Java8 - move these as default functions to VertexSender interface
  protected final void reduceDouble(
      ReducerHandle<DoubleWritable, ?> reduceHandle, double value) {
    reduceUtils.reduceDouble(reduceHandle, value);
  }

  protected final void reduceFloat(
      ReducerHandle<FloatWritable, ?> reduceHandle, float value) {
    reduceUtils.reduceFloat(reduceHandle, value);
  }

  protected final void reduceLong(
      ReducerHandle<LongWritable, ?> reduceHandle, long value) {
    reduceUtils.reduceLong(reduceHandle, value);
  }

  protected final void reduceInt(
      ReducerHandle<IntWritable, ?> reduceHandle, int value) {
    reduceUtils.reduceInt(reduceHandle, value);
  }
}
