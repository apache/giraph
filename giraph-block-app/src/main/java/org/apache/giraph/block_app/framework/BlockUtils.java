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
package org.apache.giraph.block_app.framework;

import static org.apache.giraph.utils.ReflectionUtils.getTypeArguments;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.giraph.block_app.framework.api.giraph.BlockComputation;
import org.apache.giraph.block_app.framework.api.giraph.BlockMasterCompute;
import org.apache.giraph.block_app.framework.api.giraph.BlockWorkerContext;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.PieceCount;
import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Utility functions for block applications
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class BlockUtils {
  /** Property describing BlockFactory to use for current application run */
  public static final ClassConfOption<BlockFactory> BLOCK_FACTORY_CLASS =
      ClassConfOption.create("digraph.block_factory", null, BlockFactory.class,
          "block factory describing giraph job");

  /** Property describing block worker context value class to use */
  public static final ClassConfOption<Object> BLOCK_WORKER_CONTEXT_VALUE_CLASS =
      ClassConfOption.create(
          "digraph.block_worker_context_value_class",
          Object.class, Object.class,
          "block worker context value class");

  /** Property describing whether to log execution status as application runs */
  public static final
  BooleanConfOption LOG_EXECUTION_STATUS = new BooleanConfOption(
      "giraph.block_utils.log_execution_status", true,
      "Log execution status (of which pieces are being executed, etc)");

  private static final Logger LOG = Logger.getLogger(BlockUtils.class);

  /** Dissallow constructor */
  private BlockUtils() { }

  /**
   * Create new BlockFactory that is specified in the configuration.
   */
  public static <S> BlockFactory<S> createBlockFactory(Configuration conf) {
    return ReflectionUtils.newInstance(BLOCK_FACTORY_CLASS.get(conf));
  }

  /**
   * Set which BlockFactory class to be used for the application.
   * (generally useful within tests only)
   */
  public static void setBlockFactoryClass(Configuration conf,
      Class<? extends BlockFactory<?>> clazz) {
    BLOCK_FACTORY_CLASS.set(conf, clazz);
  }

  /**
   * Set block factory, and initialize configs with it.
   * Should be used only if there are no configuration options set after
   * this method call.
   */
  public static void setAndInitBlockFactoryClass(GiraphConfiguration conf,
      Class<? extends BlockFactory<?>> clazz) {
    BLOCK_FACTORY_CLASS.set(conf, clazz);
    initAndCheckConfig(conf);
  }

  /**
   * Initializes configuration, such that running it executes block application.
   *
   * Additionally, checks types of all pieces with a block application.
   */
  public static void initAndCheckConfig(GiraphConfiguration conf) {
    conf.setMasterComputeClass(BlockMasterCompute.class);
    conf.setComputationClass(BlockComputation.class);
    conf.setWorkerContextClass(BlockWorkerContext.class);

    Preconditions.checkState(
        GiraphConstants.OUTGOING_MESSAGE_VALUE_CLASS.get(conf) == null,
        "Message types should only be specified in Pieces, " +
        "but outgoing was specified globally");
    Preconditions.checkState(
        GiraphConstants.OUTGOING_MESSAGE_VALUE_FACTORY_CLASS
          .isDefaultValue(conf),
        "Message types should only be specified in Pieces, " +
        "but factory was specified globally");
    Preconditions.checkState(
        GiraphConstants.MESSAGE_COMBINER_CLASS.get(conf) == null,
        "Message combiner should only be specified in Pieces, " +
        "but was specified globally");

    BlockFactory<?> blockFactory = createBlockFactory(conf);
    blockFactory.initConfig(conf);

    Preconditions.checkState(
        GiraphConstants.OUTGOING_MESSAGE_VALUE_CLASS.get(conf) == null,
        "Outgoing message type was specified in blockFactory.initConfig");
    Preconditions.checkState(
        GiraphConstants.OUTGOING_MESSAGE_VALUE_FACTORY_CLASS
          .isDefaultValue(conf),
        "Outgoing message factory type was specified in " +
        "blockFactory.initConfig");
    Preconditions.checkState(
        GiraphConstants.MESSAGE_COMBINER_CLASS.get(conf) == null,
        "Message combiner type was specified in blockFactory.initConfig");

    GiraphConstants.OUTGOING_MESSAGE_VALUE_CLASS.set(conf, NoMessage.class);

    final ImmutableClassesGiraphConfiguration immConf =
        new ImmutableClassesGiraphConfiguration<>(conf);

    // Create blocks to detect issues before creating a Giraph job
    // They will not be used here
    Block executionBlock = blockFactory.createBlock(immConf);
    checkBlockTypes(
        executionBlock, blockFactory.createExecutionStage(immConf), immConf);

    PieceCount pieceCount = executionBlock.getPieceCount();
    if (pieceCount.isKnown()) {
      GiraphConstants.SUPERSTEP_COUNT.set(conf, pieceCount.getCount() + 1);
    }

    // check for non 'static final' fields in BlockFactories
    Class<?> bfClass = blockFactory.getClass();
    while (!bfClass.equals(Object.class)) {
      for (Field field : bfClass.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers()) ||
            !Modifier.isFinal(field.getModifiers())) {
          throw new IllegalStateException("BlockFactory (" + bfClass +
              ") cannot have any mutable (non 'static final') fields as a " +
              "safety measure, as createBlock function is called from a " +
              "different context then all other functions, use conf argument " +
              "instead, or make it 'static final'. Field present: " + field);
        }
      }
      bfClass = bfClass.getSuperclass();
    }

    // Register outputs
    blockFactory.registerOutputs(conf);
  }

  public static void checkBlockTypes(
      Block executionBlock, Object executionStage,
      final ImmutableClassesGiraphConfiguration conf) {
    LOG.info("Executing application - " + executionBlock);

    final Class<?> vertexIdClass = conf.getVertexIdClass();
    final Class<?> vertexValueClass = conf.getVertexValueClass();
    final Class<?> edgeValueClass = conf.getEdgeValueClass();
    final Class<?> workerContextValueClass =
        BLOCK_WORKER_CONTEXT_VALUE_CLASS.get(conf);
    final Class<?> executionStageClass = executionStage.getClass();

    // Check for type inconsistencies
    executionBlock.forAllPossiblePieces(new Consumer<AbstractPiece>() {
      @Override
      public void apply(AbstractPiece piece) {
        if (!piece.getClass().equals(Piece.class)) {
          Class<?>[] classList = getTypeArguments(
              AbstractPiece.class, piece.getClass());
          Preconditions.checkArgument(classList.length == 7);

          ReflectionUtils.verifyTypes(
              vertexIdClass, classList[0], "vertexId", piece.getClass());
          ReflectionUtils.verifyTypes(
              vertexValueClass, classList[1], "vertexValue", piece.getClass());
          ReflectionUtils.verifyTypes(
              edgeValueClass, classList[2], "edgeValue", piece.getClass());

          MessageClasses classes = piece.getMessageClasses(conf);
          Class<?> messageType = classes.getMessageClass();
          if (messageType == null) {
            messageType = NoMessage.class;
          }
          ReflectionUtils.verifyTypes(
              messageType, classList[3], "message", piece.getClass());

          ReflectionUtils.verifyTypes(
              workerContextValueClass, classList[4],
              "workerContextValue", piece.getClass());
          // No need to check worker context message class at all

          ReflectionUtils.verifyTypes(
              executionStageClass, classList[6],
              "executionStage", piece.getClass());
        }
      }
    });
  }
}
